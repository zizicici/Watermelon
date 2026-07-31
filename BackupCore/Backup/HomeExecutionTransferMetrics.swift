import Foundation

struct HomeExecutionTransferMetrics: Equatable {
    let progressFraction: Double?
    let speedBytesPerSecond: Double?
    let remainingTimeSeconds: TimeInterval?

    static let inactive = HomeExecutionTransferMetrics(
        progressFraction: nil,
        speedBytesPerSecond: nil,
        remainingTimeSeconds: nil
    )
}

struct HomeExecutionTransferTracker {
    private struct ResourceKey: Hashable {
        let kind: BackupTransferKind
        let assetLocalIdentifier: String
        let resourceDisplayName: String
        let resourcePosition: Int
        let totalResources: Int
    }

    private struct ResourceProgress {
        var committedBytes: Int64 = 0
        var lastAttemptBytes: Int64 = 0
    }

    private struct Sample {
        let timestamp: CFAbsoluteTime
        let bytes: Int64
    }

    private struct RateSnapshot {
        let bytesPerSecond: Double
        let timestamp: CFAbsoluteTime
    }

    private var totalBytes: Int64?
    private var progressByKey: [ResourceKey: ResourceProgress] = [:]
    private var actualTransferredBytes: Int64 = 0
    private var samples: [Sample] = []
    private var lastProgressAt: CFAbsoluteTime?
    private var smoothedRateBytesPerSecond: Double?
    private var smoothedRateSampleTimestamp: CFAbsoluteTime?

    private static let sampleWindow: CFAbsoluteTime = 10
    private static let minimumRateInterval: CFAbsoluteTime = 1
    private static let recentProgressWindow: CFAbsoluteTime = 10
    private static let rateSmoothingTimeConstant: CFAbsoluteTime = 6

    mutating func updateTotalBytes(_ totalBytes: Int64?) {
        self.totalBytes = totalBytes
    }

    mutating func clear() {
        totalBytes = nil
        progressByKey.removeAll(keepingCapacity: false)
        actualTransferredBytes = 0
        samples.removeAll(keepingCapacity: false)
        lastProgressAt = nil
        smoothedRateBytesPerSecond = nil
        smoothedRateSampleTimestamp = nil
    }

    mutating func record(
        _ state: BackupTransferState,
        now: CFAbsoluteTime
    ) -> HomeExecutionTransferMetrics {
        let key = ResourceKey(
            kind: state.kind,
            assetLocalIdentifier: state.assetLocalIdentifier,
            resourceDisplayName: state.resourceDisplayName,
            resourcePosition: state.resourcePosition,
            totalResources: state.totalResources
        )
        let resolvedBytes = resolvedTransferredBytes(for: state)
        if let resolvedBytes {
            var progress = progressByKey[key] ?? ResourceProgress()
            let actualDelta: Int64
            if state.countsTowardTransferSpeed {
                actualDelta = resolvedBytes >= progress.lastAttemptBytes
                    ? resolvedBytes - progress.lastAttemptBytes
                    : resolvedBytes
            } else {
                actualDelta = 0
            }
            progress.lastAttemptBytes = resolvedBytes

            var committedBytes = max(
                progress.committedBytes,
                resolvedBytes
            )
            if state.resourceFraction >= 1,
               let total = state.resourceTotalBytes {
                committedBytes = max(committedBytes, total)
            }
            progress.committedBytes = committedBytes
            progressByKey[key] = progress

            if actualDelta > 0 {
                let (nextTransferredBytes, overflowed) =
                    actualTransferredBytes.addingReportingOverflow(
                        actualDelta
                    )
                actualTransferredBytes = overflowed
                    ? .max
                    : nextTransferredBytes
                lastProgressAt = now
                appendSample(now: now)
            }
        }
        return snapshot(now: now)
    }

    mutating func snapshot(
        now: CFAbsoluteTime
    ) -> HomeExecutionTransferMetrics {
        trimSamples(referenceTime: samples.last?.timestamp ?? now)
        let progressFraction = currentProgressFraction()
        guard let lastProgressAt,
              now - lastProgressAt <= Self.recentProgressWindow else {
            smoothedRateBytesPerSecond = nil
            smoothedRateSampleTimestamp = nil
            return HomeExecutionTransferMetrics(
                progressFraction: progressFraction,
                speedBytesPerSecond: nil,
                remainingTimeSeconds: nil
            )
        }
        guard let rateSnapshot = currentRate(),
              rateSnapshot.bytesPerSecond > 0 else {
            return HomeExecutionTransferMetrics(
                progressFraction: progressFraction,
                speedBytesPerSecond: nil,
                remainingTimeSeconds: nil
            )
        }
        let rate = smoothedRate(for: rateSnapshot)

        let remainingTimeSeconds: TimeInterval?
        if let totalBytes {
            let remainingBytes = max(
                0,
                totalBytes - currentAggregateBytes()
            )
            remainingTimeSeconds = Double(remainingBytes) / rate
        } else {
            remainingTimeSeconds = nil
        }
        return HomeExecutionTransferMetrics(
            progressFraction: progressFraction,
            speedBytesPerSecond: rate,
            remainingTimeSeconds: remainingTimeSeconds
        )
    }

    static func resolvedTotalBytes(
        uploadBytes: Int64?,
        downloadBytes: Int64?
    ) -> Int64? {
        guard let uploadBytes, let downloadBytes else { return nil }
        let (totalBytes, overflowed) =
            uploadBytes.addingReportingOverflow(downloadBytes)
        guard !overflowed else { return nil }
        return totalBytes > 0 ? totalBytes : nil
    }

    private func resolvedTransferredBytes(
        for state: BackupTransferState
    ) -> Int64? {
        if let resourceBytesTransferred =
            state.resourceBytesTransferred {
            if let total = state.resourceTotalBytes, total > 0 {
                return min(max(resourceBytesTransferred, 0), total)
            }
            return max(resourceBytesTransferred, 0)
        }
        guard let total = state.resourceTotalBytes,
              total > 0 else {
            return nil
        }
        let fraction = Double(state.resourceFraction)
        guard fraction.isFinite else { return nil }
        if fraction <= 0 { return 0 }
        if fraction >= 1 { return total }
        return Int64(
            exactly: (Double(total) * fraction).rounded()
        )
    }

    private func currentAggregateBytes() -> Int64 {
        progressByKey.values.reduce(Int64(0)) { total, progress in
            let (nextTotal, overflowed) =
                total.addingReportingOverflow(
                    progress.committedBytes
                )
            return overflowed ? .max : nextTotal
        }
    }

    private func currentProgressFraction() -> Double? {
        guard let totalBytes, totalBytes > 0 else { return nil }
        return min(
            max(
                Double(currentAggregateBytes())
                    / Double(totalBytes),
                0
            ),
            1
        )
    }

    private mutating func appendSample(now: CFAbsoluteTime) {
        samples.append(
            Sample(
                timestamp: now,
                bytes: actualTransferredBytes
            )
        )
        trimSamples(referenceTime: now)
    }

    private mutating func trimSamples(
        referenceTime: CFAbsoluteTime
    ) {
        samples.removeAll {
            referenceTime - $0.timestamp > Self.sampleWindow
        }
    }

    private func currentRate() -> RateSnapshot? {
        guard let last = samples.last else { return nil }
        guard let baseline = samples.dropLast().first(
            where: {
                last.timestamp - $0.timestamp
                    >= Self.minimumRateInterval
            }
        ) else {
            return nil
        }
        let elapsed = last.timestamp - baseline.timestamp
        guard elapsed >= Self.minimumRateInterval else { return nil }
        let delta = last.bytes - baseline.bytes
        guard delta > 0 else { return nil }
        return RateSnapshot(
            bytesPerSecond: Double(delta) / elapsed,
            timestamp: last.timestamp
        )
    }

    private mutating func smoothedRate(
        for rate: RateSnapshot
    ) -> Double {
        guard let previousRate = smoothedRateBytesPerSecond,
              let previousTimestamp =
                smoothedRateSampleTimestamp else {
            smoothedRateBytesPerSecond = rate.bytesPerSecond
            smoothedRateSampleTimestamp = rate.timestamp
            return rate.bytesPerSecond
        }
        guard rate.timestamp > previousTimestamp else {
            return previousRate
        }
        let elapsed = rate.timestamp - previousTimestamp
        let alpha = min(
            max(
                1 - exp(
                    -elapsed
                        / Self.rateSmoothingTimeConstant
                ),
                0
            ),
            1
        )
        let nextRate =
            previousRate
            + (rate.bytesPerSecond - previousRate) * alpha
        smoothedRateBytesPerSecond = nextRate
        smoothedRateSampleTimestamp = rate.timestamp
        return nextRate
    }
}

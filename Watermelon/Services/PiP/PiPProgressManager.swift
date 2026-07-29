import AVFoundation
import AVKit
import MoreKit
import UIKit

@MainActor
final class PiPProgressManager: NSObject {

    static let shared = PiPProgressManager()

    private static let pipSize = CGSize(width: 480, height: 360)
    private static let refreshInterval: TimeInterval = 1.0

    private var pipController: AVPictureInPictureController?
    private var displayLayer: AVSampleBufferDisplayLayer?
    private var pipSourceView: UIView?
    private var refreshTimer: Timer?
    private var ambientPlayer: AVAudioPlayer?
    private var chimePlayer: AVAudioPlayer?

    private(set) var isActive = false
    private var hasActiveTask = false
    private var isPiPShowing = false
    private var isFinished = false
    private var isPaused = false

    private var currentStatusText = ""
    private var currentElapsedText = ""
    private var currentTransferMetrics = HomeExecutionTransferMetrics.inactive
    private var progressAccumulator = PiPProgressAccumulator()
    private var elapsedTimeTracker = PiPElapsedTimeTracker()
    private var finishedStatusText: String?
    private var finishedStatusTone: FinishTone = .neutral

    private enum FinishTone {
        case success
        case failure
        case neutral
    }

    var isEnabled: Bool { PiPProgressSetting.getValue() == .enable && ProStatus.isPro }

    private override init() {
        super.init()

        NotificationCenter.default.addObserver(
            self,
            selector: #selector(appWillResignActive),
            name: UIApplication.willResignActiveNotification,
            object: nil
        )
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(appDidBecomeActive),
            name: UIApplication.didBecomeActiveNotification,
            object: nil
        )
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(settingsDidUpdate),
            name: .SettingsUpdate,
            object: nil
        )
    }

    // MARK: - Public API

    func taskDidStart(title: String) {
        guard isEnabled, AVPictureInPictureController.isPictureInPictureSupported() else { return }
        hasActiveTask = true
        isFinished = false
        finishedStatusText = nil
        finishedStatusTone = .neutral
        currentStatusText = title
        currentTransferMetrics = .inactive
        progressAccumulator.reset()
        isPaused = false
        elapsedTimeTracker.start(at: monotonicNow)
        currentElapsedText = formattedElapsed(0)
        if !isActive {
            preparePiPController()
        } else {
            pushFrame()
        }
    }

    func updateStatus(_ text: String) {
        guard hasActiveTask else { return }
        currentStatusText = text
        if isActive { pushFrame() }
    }

    func updateTransferMetrics(_ metrics: HomeExecutionTransferMetrics) {
        guard hasActiveTask else { return }
        currentTransferMetrics = metrics
        progressAccumulator.update(metrics.progressFraction)
        if isActive { pushFrame() }
    }

    func taskDidResume() {
        guard hasActiveTask else { return }
        progressAccumulator.beginRemainingSegment()
        currentTransferMetrics = .inactive
    }

    func setPaused(_ paused: Bool) {
        guard hasActiveTask else { return }
        let now = monotonicNow
        elapsedTimeTracker.setPaused(paused, at: now)
        currentElapsedText = formattedElapsed(elapsedTimeTracker.elapsed(at: now))
        isPaused = paused
        if isActive { pushFrame() }
    }

    func taskDidComplete() {
        progressAccumulator.complete()
        finishPiP(statusText: String(localized: "pip.status.completed"), tone: .success)
    }

    func taskDidFail() {
        progressAccumulator.freeze()
        finishPiP(statusText: String(localized: "pip.status.failed"), tone: .failure)
    }

    func taskDidCancel() {
        progressAccumulator.freeze()
        finishPiP(statusText: String(localized: "pip.status.cancelled"), tone: .neutral)
    }

    // MARK: - Lifecycle

    @objc private func appWillResignActive() {
        guard isActive, isEnabled, hasActiveTask else { return }
        pipController?.startPictureInPicture()
    }

    @objc private func appDidBecomeActive() {
        guard isPiPShowing else { return }
        stopAmbientLoop()
        pipController?.stopPictureInPicture()
        pipSourceView?.removeFromSuperview()

        pipSourceView = nil
        pipController = nil
        displayLayer = nil
        isActive = false
        isPiPShowing = false

        if hasActiveTask {
            preparePiPController()
        } else {
            resetDisplayState()
        }
    }

    @objc private func settingsDidUpdate() {
        if let player = ambientPlayer {
            player.volume = ambientVolume
            return
        }
        if isPiPShowing, !isFinished {
            startAmbientLoop()
        }
    }

    private func finishPiP(statusText: String, tone: FinishTone) {
        hasActiveTask = false
        guard isActive, !isFinished else { return }

        if isPiPShowing {
            isFinished = true
            finishedStatusText = statusText
            finishedStatusTone = tone
            freezeElapsedTime()
            stopAmbientLoop()
            stopRefreshTimer()
            pushFrame()
            if tone != .neutral { playCompletionChime() }
        } else {
            if tone != .neutral { playCompletionChime() }
            tearDown()
        }
    }

    private func freezeElapsedTime() {
        currentElapsedText = formattedElapsed(elapsedTimeTracker.stop(at: monotonicNow))
    }

    // MARK: - PiP Infrastructure

    private func preparePiPController() {
        guard pipController == nil else { return }

        configureAudioSession()

        let layer = AVSampleBufferDisplayLayer()
        layer.frame = CGRect(origin: .zero, size: Self.pipSize)
        layer.videoGravity = .resizeAspect

        let sourceView = UIView(frame: CGRect(origin: .zero, size: Self.pipSize))
        sourceView.isUserInteractionEnabled = false
        sourceView.layer.addSublayer(layer)

        guard let windowScene = UIApplication.shared.connectedScenes
            .compactMap({ $0 as? UIWindowScene }).first,
              let window = windowScene.windows.first else {
            return
        }
        window.insertSubview(sourceView, at: 0)

        let contentSource = AVPictureInPictureController.ContentSource(
            sampleBufferDisplayLayer: layer,
            playbackDelegate: self
        )
        let controller = AVPictureInPictureController(contentSource: contentSource)
        controller.delegate = self
        controller.canStartPictureInPictureAutomaticallyFromInline = true
        controller.requiresLinearPlayback = true
        // Hide the system play/pause/scrub chrome so only our custom render shows.
        controller.setValue(2, forKey: "controlsStyle")

        displayLayer = layer
        pipSourceView = sourceView
        pipController = controller
        isActive = true

        pushFrame()
        startRefreshTimer()
    }

    private func tearDown() {
        stopRefreshTimer()
        stopAmbientLoop()

        if let pip = pipController, pip.isPictureInPictureActive {
            pip.stopPictureInPicture()
        }

        isActive = false
        isPiPShowing = false
        hasActiveTask = false
        resetDisplayState()

        displayLayer?.flushAndRemoveImage()
        pipSourceView?.removeFromSuperview()
        pipSourceView = nil
        displayLayer = nil
        pipController = nil
    }

    private func resetDisplayState() {
        isFinished = false
        finishedStatusText = nil
        finishedStatusTone = .neutral
        currentStatusText = ""
        currentElapsedText = ""
        currentTransferMetrics = .inactive
        progressAccumulator.reset()
        elapsedTimeTracker.reset()
        isPaused = false
    }

    // MARK: - Audio

    private func configureAudioSession() {
        let session = AVAudioSession.sharedInstance()
        try? session.setCategory(.playback, mode: .moviePlayback, options: [.mixWithOthers])
        try? session.setActive(true)
    }

    private func startAmbientLoop() {
        guard ambientPlayer == nil, !isFinished,
              let url = Bundle.main.url(forResource: "keyboard-typing", withExtension: "mp3") else {
            return
        }
        do {
            let player = try AVAudioPlayer(contentsOf: url)
            player.numberOfLoops = -1
            player.volume = ambientVolume
            player.play()
            ambientPlayer = player
        } catch {}
    }

    // Keep playback active for PiP/background continuity when sound is off.
    private var ambientVolume: Float {
        PiPProgressSoundSetting.getValue().playsKeyboardSound ? 0.15 : 0.00001
    }

    private func stopAmbientLoop() {
        ambientPlayer?.stop()
        ambientPlayer = nil
    }

    private func playCompletionChime() {
        guard let url = Bundle.main.url(forResource: "complete", withExtension: "wav") else { return }
        do {
            let player = try AVAudioPlayer(contentsOf: url)
            player.volume = 0.4
            player.play()
            chimePlayer = player
        } catch {}
    }

    // MARK: - Timer

    private func startRefreshTimer() {
        stopRefreshTimer()
        let timer = Timer(timeInterval: Self.refreshInterval, repeats: true) { [weak self] _ in
            Task { @MainActor [weak self] in
                self?.timerTick()
            }
        }
        refreshTimer = timer
        RunLoop.main.add(timer, forMode: .common)
    }

    private func stopRefreshTimer() {
        refreshTimer?.invalidate()
        refreshTimer = nil
    }

    private func timerTick() {
        currentElapsedText = formattedElapsed(elapsedTimeTracker.elapsed(at: monotonicNow))
        pushFrame()
    }

    private func formattedElapsed(_ interval: TimeInterval) -> String {
        let roundedInterval = max(0, interval).rounded(.down)
        let totalSeconds = Int(exactly: roundedInterval) ?? .max
        let hours = totalSeconds / 3600
        let minutes = (totalSeconds % 3600) / 60
        let seconds = totalSeconds % 60
        if hours > 0 {
            return String(format: "%d:%02d:%02d", hours, minutes, seconds)
        }
        return String(format: "%d:%02d", minutes, seconds)
    }

    // MARK: - Frame Rendering

    private func pushFrame() {
        guard let layer = displayLayer else { return }
        guard let pixelBuffer = renderPixelBuffer() else { return }

        var timingInfo = CMSampleTimingInfo(
            duration: CMTime(seconds: Self.refreshInterval, preferredTimescale: 600),
            presentationTimeStamp: CMClockGetTime(CMClockGetHostTimeClock()),
            decodeTimeStamp: .invalid
        )

        var formatDescription: CMFormatDescription?
        CMVideoFormatDescriptionCreateForImageBuffer(
            allocator: kCFAllocatorDefault,
            imageBuffer: pixelBuffer,
            formatDescriptionOut: &formatDescription
        )
        guard let format = formatDescription else { return }

        var sampleBuffer: CMSampleBuffer?
        CMSampleBufferCreateReadyWithImageBuffer(
            allocator: kCFAllocatorDefault,
            imageBuffer: pixelBuffer,
            formatDescription: format,
            sampleTiming: &timingInfo,
            sampleBufferOut: &sampleBuffer
        )
        guard let buffer = sampleBuffer else { return }

        layer.flush()
        layer.enqueue(buffer)
    }

    private func renderPixelBuffer() -> CVPixelBuffer? {
        let scale: CGFloat = 1
        let width = Int(Self.pipSize.width)
        let height = Int(Self.pipSize.height)

        let attrs: [CFString: Any] = [
            kCVPixelBufferCGImageCompatibilityKey: true,
            kCVPixelBufferCGBitmapContextCompatibilityKey: true,
            kCVPixelBufferIOSurfacePropertiesKey: [:] as CFDictionary,
        ]
        var pixelBuffer: CVPixelBuffer?
        CVPixelBufferCreate(
            kCFAllocatorDefault,
            width, height,
            kCVPixelFormatType_32BGRA,
            attrs as CFDictionary,
            &pixelBuffer
        )
        guard let buffer = pixelBuffer else { return nil }

        guard CVPixelBufferLockBaseAddress(buffer, []) == kCVReturnSuccess else { return nil }
        defer { CVPixelBufferUnlockBaseAddress(buffer, []) }

        guard let data = CVPixelBufferGetBaseAddress(buffer) else { return nil }
        let colorSpace = CGColorSpaceCreateDeviceRGB()
        guard let ctx = CGContext(
            data: data,
            width: width,
            height: height,
            bitsPerComponent: 8,
            bytesPerRow: CVPixelBufferGetBytesPerRow(buffer),
            space: colorSpace,
            bitmapInfo: CGBitmapInfo.byteOrder32Little.rawValue | CGImageAlphaInfo.premultipliedFirst.rawValue
        ) else { return nil }

        ctx.translateBy(x: 0, y: CGFloat(height))
        ctx.scaleBy(x: scale, y: -scale)

        UIGraphicsPushContext(ctx)
        drawStatusContent()
        UIGraphicsPopContext()

        return buffer
    }

    private func drawStatusContent() {
        let palette = displayPalette()
        palette.surface.setFill()
        UIRectFill(CGRect(origin: .zero, size: Self.pipSize))

        drawStatusContent(palette: palette)
    }

    private func drawStatusContent(palette: DisplayPalette) {
        let padding: CGFloat = 48
        let contentWidth = Self.pipSize.width - padding * 2

        drawStatusLine(
            finishedStatusText ?? currentStatusText,
            color: palette.text,
            accent: palette.accent,
            rect: CGRect(x: padding, y: 52, width: contentWidth, height: 38),
            baseFontSize: 28,
            minimumFontSize: 18,
            dotSize: 10
        )

        let progress = progressPresentation()
        drawCentered(
            progress.text,
            baseFontSize: progress.fontSize,
            weight: .medium,
            monospacedDigits: progress.monospacedDigits,
            color: palette.text,
            rect: CGRect(x: padding, y: 112, width: contentWidth, height: 88),
            minimumFontSize: 24
        )

        drawProgressTrack(
            fraction: progressAccumulator.displayedFraction,
            trackColor: palette.track,
            fillColor: palette.accent,
            rect: CGRect(x: padding, y: 239, width: contentWidth, height: 8)
        )

        drawCentered(
            metricText(),
            baseFontSize: 24,
            weight: .regular,
            monospacedDigits: false,
            color: palette.secondaryText,
            rect: CGRect(x: padding, y: 276, width: contentWidth, height: 32),
            minimumFontSize: 16
        )
    }

    private func progressPresentation() -> (text: String, fontSize: CGFloat, monospacedDigits: Bool) {
        if let fraction = progressAccumulator.displayedFraction {
            return (
                String(format: "%.0f%%", fraction * 100),
                72,
                true
            )
        }
        return (
            String(localized: "log.transfer.estimating"),
            36,
            false
        )
    }

    private func drawStatusLine(
        _ text: String,
        color: UIColor,
        accent: UIColor,
        rect: CGRect,
        baseFontSize: CGFloat,
        minimumFontSize: CGFloat,
        dotSize: CGFloat
    ) {
        let gap: CGFloat = 10
        let maxTextWidth = rect.width - dotSize - gap
        let font = fittingFont(
            text: text,
            baseSize: baseFontSize,
            minimumSize: minimumFontSize,
            weight: .medium,
            maxWidth: maxTextWidth
        )
        let attributes: [NSAttributedString.Key: Any] = [.font: font, .foregroundColor: color]
        let textSize = (text as NSString).size(withAttributes: attributes)
        let textWidth = min(textSize.width, maxTextWidth)
        let totalWidth = dotSize + gap + textWidth
        let originX = rect.midX - totalWidth / 2
        let dotRect = CGRect(
            x: originX,
            y: rect.midY - dotSize / 2,
            width: dotSize,
            height: dotSize
        )
        accent.setFill()
        UIBezierPath(ovalIn: dotRect).fill()
        (text as NSString).draw(
            with: CGRect(
                x: originX + dotSize + gap,
                y: rect.midY - font.lineHeight / 2,
                width: textWidth,
                height: font.lineHeight
            ),
            options: [.usesLineFragmentOrigin, .truncatesLastVisibleLine],
            attributes: attributes,
            context: nil
        )
    }

    private func drawProgressTrack(
        fraction: Double?,
        trackColor: UIColor,
        fillColor: UIColor,
        rect: CGRect
    ) {
        trackColor.setFill()
        UIBezierPath(roundedRect: rect, cornerRadius: rect.height / 2).fill()
        guard let fraction, fraction.isFinite, fraction > 0 else { return }
        let fillWidth = rect.width * min(max(fraction, 0), 1)
        let fillRect = CGRect(x: rect.minX, y: rect.minY, width: fillWidth, height: rect.height)
        fillColor.setFill()
        UIBezierPath(
            roundedRect: fillRect,
            cornerRadius: min(rect.height / 2, fillWidth / 2)
        ).fill()
    }

    private func drawCentered(
        _ text: String,
        baseFontSize: CGFloat,
        weight: UIFont.Weight,
        monospacedDigits: Bool,
        color: UIColor,
        rect: CGRect,
        minimumFontSize: CGFloat
    ) {
        let resolvedFont = fittingFont(
            text: text,
            baseSize: baseFontSize,
            minimumSize: minimumFontSize,
            weight: weight,
            maxWidth: rect.width,
            monospacedDigits: monospacedDigits
        )
        let attributes: [NSAttributedString.Key: Any] = [.font: resolvedFont, .foregroundColor: color]
        (text as NSString).draw(
            with: CGRect(
                x: rect.minX,
                y: rect.midY - resolvedFont.lineHeight / 2,
                width: rect.width,
                height: resolvedFont.lineHeight
            ),
            options: [.usesLineFragmentOrigin, .truncatesLastVisibleLine],
            attributes: attributes.merging([.paragraphStyle: centeredParagraphStyle]) { current, _ in current },
            context: nil
        )
    }

    private var centeredParagraphStyle: NSParagraphStyle {
        let style = NSMutableParagraphStyle()
        style.alignment = .center
        style.lineBreakMode = .byTruncatingTail
        return style
    }

    private func fittingFont(
        text: String,
        baseSize: CGFloat,
        minimumSize: CGFloat,
        weight: UIFont.Weight,
        maxWidth: CGFloat,
        monospacedDigits: Bool = false
    ) -> UIFont {
        var size = baseSize
        while size > minimumSize {
            let font = monospacedDigits
                ? UIFont.monospacedDigitSystemFont(ofSize: size, weight: weight)
                : UIFont.systemFont(ofSize: size, weight: weight)
            if (text as NSString).size(withAttributes: [.font: font]).width <= maxWidth {
                return font
            }
            size -= 1
        }
        return monospacedDigits
            ? UIFont.monospacedDigitSystemFont(ofSize: minimumSize, weight: weight)
            : UIFont.systemFont(ofSize: minimumSize, weight: weight)
    }

    private func metricText() -> String {
        if isFinished || isPaused {
            return currentElapsedText
        }
        let metrics = activeMetricComponents()
        return "\(metrics.speed) · \(metrics.remaining)"
    }

    private func activeMetricComponents() -> (speed: String, remaining: String) {
        let speed = HomeExecutionTransferFormatter.speed(currentTransferMetrics.speedBytesPerSecond)
            ?? String(localized: "log.transfer.waiting")
        let remaining = HomeExecutionTransferFormatter.remainingTime(currentTransferMetrics.remainingTimeSeconds)
            .map { "≈ \($0)" }
            ?? String(localized: "log.transfer.estimating")
        return (speed, remaining)
    }

    private func displayPalette() -> DisplayPalette {
        let traits = pipSourceView?.traitCollection ?? UIScreen.main.traitCollection
        let surface = UIColor.materialSurface(
            light: .Material.Green._100,
            darkTint: .Material.Green._200,
            darkAlpha: 0.16
        ).resolvedColor(with: traits)
        let text = UIColor.materialOnContainer(
            light: .Material.Green._900,
            dark: .Material.Green._100
        ).resolvedColor(with: traits)
        let secondaryText = UIColor.materialOnSurfaceVariant(
            light: .Material.Green._700,
            dark: .Material.Green._200
        ).resolvedColor(with: traits)
        let accent: UIColor
        switch finishedStatusTone {
        case .failure:
            accent = UIColor.materialPrimary(
                light: .Material.Red._600,
                dark: .Material.Red._200
            ).resolvedColor(with: traits)
        case .neutral where isFinished, .neutral where isPaused:
            accent = secondaryText
        default:
            accent = UIColor.materialPrimary(
                light: .Material.Green._600,
                dark: .Material.Green._200
            ).resolvedColor(with: traits)
        }
        return DisplayPalette(
            surface: surface,
            text: text,
            secondaryText: secondaryText,
            accent: accent,
            track: accent.withAlphaComponent(0.18)
        )
    }

    private struct DisplayPalette {
        let surface: UIColor
        let text: UIColor
        let secondaryText: UIColor
        let accent: UIColor
        let track: UIColor
    }

    private var monotonicNow: TimeInterval {
        ProcessInfo.processInfo.systemUptime
    }
}

// MARK: - AVPictureInPictureControllerDelegate

extension PiPProgressManager: AVPictureInPictureControllerDelegate {
    nonisolated func pictureInPictureControllerWillStartPictureInPicture(
        _ pictureInPictureController: AVPictureInPictureController
    ) {
        Task { @MainActor [weak self] in
            self?.isPiPShowing = true
            self?.startAmbientLoop()
        }
    }

    nonisolated func pictureInPictureControllerDidStopPictureInPicture(
        _ pictureInPictureController: AVPictureInPictureController
    ) {
        Task { @MainActor [weak self] in
            guard let self else { return }
            self.isPiPShowing = false
            self.stopAmbientLoop()
            if self.isFinished {
                self.tearDown()
            }
        }
    }

    nonisolated func pictureInPictureController(
        _ pictureInPictureController: AVPictureInPictureController,
        restoreUserInterfaceForPictureInPictureStopWithCompletionHandler completionHandler: @escaping (Bool) -> Void
    ) {
        Task { @MainActor [weak self] in
            self?.pipSourceView?.isHidden = true
            completionHandler(true)
        }
    }

    nonisolated func pictureInPictureController(
        _ pictureInPictureController: AVPictureInPictureController,
        failedToStartPictureInPictureWithError error: Error
    ) {
        Task { @MainActor [weak self] in
            guard let self else { return }
            self.isPiPShowing = false
            self.stopAmbientLoop()
        }
    }
}

// MARK: - AVPictureInPictureSampleBufferPlaybackDelegate

extension PiPProgressManager: AVPictureInPictureSampleBufferPlaybackDelegate {
    nonisolated func pictureInPictureController(
        _ pictureInPictureController: AVPictureInPictureController,
        setPlaying playing: Bool
    ) {}

    nonisolated func pictureInPictureControllerTimeRangeForPlayback(
        _ pictureInPictureController: AVPictureInPictureController
    ) -> CMTimeRange {
        CMTimeRange(start: .negativeInfinity, duration: .positiveInfinity)
    }

    nonisolated func pictureInPictureControllerIsPlaybackPaused(
        _ pictureInPictureController: AVPictureInPictureController
    ) -> Bool {
        false
    }

    nonisolated func pictureInPictureController(
        _ pictureInPictureController: AVPictureInPictureController,
        didTransitionToRenderSize newRenderSize: CMVideoDimensions
    ) {}

    nonisolated func pictureInPictureController(
        _ pictureInPictureController: AVPictureInPictureController,
        skipByInterval skipInterval: CMTime,
        completion completionHandler: @escaping () -> Void
    ) {
        completionHandler()
    }
}

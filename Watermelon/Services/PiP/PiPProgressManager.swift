import AVFoundation
import AVKit
import MoreKit
import UIKit

@MainActor
final class PiPProgressManager: NSObject {

    static let shared = PiPProgressManager()

    private static let pipSize = CGSize(width: 270, height: 480)
    private static let refreshInterval: TimeInterval = 1.0
    private static let recentEntriesCap = 60
    private static let recentEntriesSlack = 40

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

    private var currentStatusText = ""
    private var currentElapsedText = ""
    private var finishedStatusText: String?
    private var finishedStatusTone: FinishTone = .neutral
    private var taskStartDate: Date?
    private var recentEntries: [ExecutionLogEntry] = []

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
        taskStartDate = Date()
        recentEntries.removeAll(keepingCapacity: true)
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

    func appendLog(_ entry: ExecutionLogEntry) {
        guard isEnabled, hasActiveTask else { return }
        recentEntries.append(entry)
        if recentEntries.count >= Self.recentEntriesCap + Self.recentEntriesSlack {
            recentEntries.removeFirst(recentEntries.count - Self.recentEntriesCap)
        }
    }

    func taskDidComplete() {
        finishPiP(statusText: String(localized: "pip.status.completed"), tone: .success)
    }

    func taskDidFail(message: String? = nil) {
        finishPiP(statusText: message ?? String(localized: "pip.status.failed"), tone: .failure)
    }

    func taskDidCancel() {
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
        guard PiPProgressSoundSetting.getValue().playsKeyboardSound else {
            stopAmbientLoop()
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
        if let start = taskStartDate {
            currentElapsedText = formattedElapsed(Date().timeIntervalSince(start))
        }
        taskStartDate = nil
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
        taskStartDate = nil
        recentEntries.removeAll(keepingCapacity: true)
    }

    // MARK: - Audio

    private func configureAudioSession() {
        let session = AVAudioSession.sharedInstance()
        try? session.setCategory(.playback, mode: .moviePlayback, options: [.mixWithOthers])
        try? session.setActive(true)
    }

    private func startAmbientLoop() {
        guard ambientPlayer == nil, !isFinished,
              PiPProgressSoundSetting.getValue().playsKeyboardSound,
              let url = Bundle.main.url(forResource: "keyboard-typing", withExtension: "mp3") else {
            return
        }
        do {
            let player = try AVAudioPlayer(contentsOf: url)
            player.numberOfLoops = -1
            player.volume = 0.15
            player.play()
            ambientPlayer = player
        } catch {}
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
        if let start = taskStartDate {
            currentElapsedText = formattedElapsed(Date().timeIntervalSince(start))
        }
        pushFrame()
    }

    private func formattedElapsed(_ interval: TimeInterval) -> String {
        let totalSeconds = Int(interval)
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
        let scale = UIScreen.main.scale
        let width = Int(Self.pipSize.width * scale)
        let height = Int(Self.pipSize.height * scale)

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

        CVPixelBufferLockBaseAddress(buffer, [])
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
        UIColor.black.setFill()
        UIRectFill(CGRect(origin: .zero, size: Self.pipSize))

        let padding: CGFloat = 16
        let contentWidth = Self.pipSize.width - padding * 2
        var y: CGFloat = 20

        let elapsedFont = UIFont.monospacedDigitSystemFont(ofSize: 28, weight: .bold)
        let elapsedColor: UIColor = isFinished ? UIColor(white: 1, alpha: 0.55) : UIColor(white: 1, alpha: 0.92)
        (currentElapsedText as NSString).draw(
            with: CGRect(x: padding, y: y, width: contentWidth, height: 34),
            options: [.usesLineFragmentOrigin],
            attributes: [.font: elapsedFont, .foregroundColor: elapsedColor],
            context: nil
        )
        y += 38

        if let finishedText = finishedStatusText {
            let bannerColor: UIColor
            switch finishedStatusTone {
            case .success: bannerColor = UIColor.systemGreen.withAlphaComponent(0.85)
            case .failure: bannerColor = UIColor.systemRed.withAlphaComponent(0.85)
            case .neutral: bannerColor = UIColor.systemGray.withAlphaComponent(0.85)
            }
            drawBanner(text: finishedText, color: bannerColor, origin: CGPoint(x: padding, y: y), width: contentWidth)
            y += 36
        } else {
            let statusFont = UIFont.systemFont(ofSize: 13, weight: .regular)
            let statusRect = CGRect(x: padding, y: y, width: contentWidth, height: 32)
            (currentStatusText as NSString).draw(
                with: statusRect,
                options: [.usesLineFragmentOrigin, .truncatesLastVisibleLine],
                attributes: [.font: statusFont, .foregroundColor: UIColor(white: 1, alpha: 0.7)],
                context: nil
            )
            y += 36
        }

        let tailRect = CGRect(x: padding, y: y, width: contentWidth, height: Self.pipSize.height - y - padding)
        PiPLogTailRenderer.draw(entries: recentEntries, in: tailRect)
    }

    private func drawBanner(text: String, color: UIColor, origin: CGPoint, width: CGFloat) {
        let rect = CGRect(origin: origin, size: CGSize(width: width, height: 30))
        color.setFill()
        UIBezierPath(roundedRect: rect, cornerRadius: 6).fill()
        let font = UIFont.systemFont(ofSize: 14, weight: .semibold)
        let attrs: [NSAttributedString.Key: Any] = [.font: font, .foregroundColor: UIColor.white]
        let textSize = (text as NSString).size(withAttributes: attrs)
        let textPoint = CGPoint(
            x: origin.x + (width - textSize.width) / 2,
            y: origin.y + (30 - textSize.height) / 2
        )
        (text as NSString).draw(at: textPoint, withAttributes: attrs)
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

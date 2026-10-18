import UIKit
import MoreKit

final class WatermelonPaywallHeader: UIView {
    private let heading = UILabel()
    private let subtitle = UILabel()

    override init(frame: CGRect) {
        super.init(frame: frame)
        let artwork = WatermelonProArtwork()
        artwork.translatesAutoresizingMaskIntoConstraints = false
        addSubview(artwork)
        heading.font = .systemFont(ofSize: 32, weight: .semibold)
        heading.textColor = .label
        subtitle.font = .systemFont(ofSize: 16)
        subtitle.textColor = .secondaryLabel
        subtitle.numberOfLines = 0
        let text = UIStackView(arrangedSubviews: [heading, subtitle])
        text.axis = .vertical
        text.spacing = 10
        text.translatesAutoresizingMaskIntoConstraints = false
        addSubview(text)
        NSLayoutConstraint.activate([
            artwork.topAnchor.constraint(equalTo: topAnchor, constant: 72),
            artwork.centerXAnchor.constraint(equalTo: centerXAnchor),
            artwork.widthAnchor.constraint(equalToConstant: 160),
            artwork.heightAnchor.constraint(equalToConstant: 160),
            text.topAnchor.constraint(equalTo: artwork.bottomAnchor, constant: 32),
            text.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 24),
            text.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -24),
            text.bottomAnchor.constraint(equalTo: bottomAnchor)
        ])
    }

    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }

    func configure(title: String, subtitle: String) {
        heading.text = title
        self.subtitle.text = subtitle
    }
}

private final class WatermelonProArtwork: UIView {
    override init(frame: CGRect) {
        super.init(frame: frame)
        let icon = UIImageView(image: AppInfo.App.watermelon.image)
        icon.accessibilityIdentifier = "paywall.appIcon"
        icon.contentMode = .scaleAspectFit
        icon.layer.cornerRadius = 26
        icon.layer.cornerCurve = .continuous
        icon.clipsToBounds = true
        let crown = WatermelonProCrownView()
        for child in [icon, crown] {
            child.translatesAutoresizingMaskIntoConstraints = false
            addSubview(child)
        }
        NSLayoutConstraint.activate([
            icon.centerXAnchor.constraint(equalTo: centerXAnchor),
            icon.centerYAnchor.constraint(equalTo: centerYAnchor, constant: 10),
            icon.widthAnchor.constraint(equalToConstant: 112),
            icon.heightAnchor.constraint(equalToConstant: 112),
            crown.centerXAnchor.constraint(equalTo: icon.trailingAnchor, constant: -4),
            crown.centerYAnchor.constraint(equalTo: icon.topAnchor, constant: 2),
            crown.widthAnchor.constraint(equalToConstant: 48),
            crown.heightAnchor.constraint(equalToConstant: 42)
        ])
    }

    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }
}

private final class WatermelonProCrownView: UIView {
    private struct Sparkle {
        let view: WatermelonCrownSparkleView
        let horizontal: NSLayoutConstraint
        let vertical: NSLayoutConstraint
        let positions: [CGPoint]
        var position = 0
    }

    private let glowView = UIView()
    private let glow = CAGradientLayer()
    private var sparkles: [Sparkle] = []
    private var animationGeneration = 0

    override init(frame: CGRect) {
        super.init(frame: frame)
        isUserInteractionEnabled = false
        glow.type = .radial
        glow.startPoint = CGPoint(x: 0.5, y: 0.5)
        glow.endPoint = CGPoint(x: 1, y: 1)
        if #available(iOS 26.0, *) {
            glow.preferredDynamicRange = .constrainedHigh
            glow.colors = [
                UIColor(red: 1, green: 0.8, blue: 0.3, alpha: 0.5, linearExposure: 1.6).cgColor,
                UIColor(red: 1, green: 0.9, blue: 0.6, alpha: 0.18, linearExposure: 1.6).cgColor,
                UIColor(red: 1, green: 0.9, blue: 0.6, alpha: 0, linearExposure: 1.6).cgColor
            ]
        } else {
            glow.colors = [
                UIColor.Material.Amber._300.withAlphaComponent(0.5).cgColor,
                UIColor.Material.Amber._200.withAlphaComponent(0.18).cgColor,
                UIColor.Material.Amber._200.withAlphaComponent(0).cgColor
            ]
        }
        glow.locations = [0, 0.35, 1]
        glow.opacity = 0.85
        glowView.layer.addSublayer(glow)
        let crown = UIImageView(image: UIImage(systemName: "crown.fill", withConfiguration: UIImage.SymbolConfiguration(pointSize: 32, weight: .semibold)))
        crown.accessibilityIdentifier = "paywall.crown"
        crown.tintColor = .Material.Amber._500
        crown.contentMode = .scaleAspectFit
        crown.clipsToBounds = false
        crown.layer.shadowColor = UIColor.Material.Amber._100.cgColor
        crown.layer.shadowOpacity = 1
        crown.layer.shadowRadius = 9
        crown.layer.shadowOffset = CGSize(width: 0, height: 1)
        crown.transform = CGAffineTransform(rotationAngle: 25 * .pi / 180)
        for child in [glowView, crown] {
            child.translatesAutoresizingMaskIntoConstraints = false
            addSubview(child)
        }
        NSLayoutConstraint.activate([
            glowView.centerXAnchor.constraint(equalTo: centerXAnchor),
            glowView.centerYAnchor.constraint(equalTo: centerYAnchor),
            glowView.widthAnchor.constraint(equalToConstant: 104),
            glowView.heightAnchor.constraint(equalToConstant: 104),
            crown.centerXAnchor.constraint(equalTo: centerXAnchor),
            crown.centerYAnchor.constraint(equalTo: centerYAnchor),
            crown.widthAnchor.constraint(equalTo: widthAnchor),
            crown.heightAnchor.constraint(equalTo: heightAnchor)
        ])
        let positions: [[CGPoint]] = [
            [.init(x: 32, y: -20), .init(x: 42, y: -10), .init(x: 24, y: -33), .init(x: 38, y: -28)],
            [.init(x: -27, y: -19), .init(x: -40, y: -7), .init(x: -34, y: -28), .init(x: -20, y: -32)],
            [.init(x: 20, y: 29), .init(x: 36, y: 14), .init(x: 31, y: 27), .init(x: 7, y: 35)]
        ]
        for (index, size) in [CGFloat(14), 9, 11].enumerated() {
            let sparkle = WatermelonCrownSparkleView()
            sparkle.accessibilityIdentifier = "paywall.sparkle.\(index)"
            sparkle.alpha = 0.85
            sparkle.translatesAutoresizingMaskIntoConstraints = false
            addSubview(sparkle)
            let horizontal = sparkle.centerXAnchor.constraint(equalTo: centerXAnchor, constant: positions[index][0].x)
            let vertical = sparkle.centerYAnchor.constraint(equalTo: centerYAnchor, constant: positions[index][0].y)
            NSLayoutConstraint.activate([
                horizontal, vertical,
                sparkle.widthAnchor.constraint(equalToConstant: size),
                sparkle.heightAnchor.constraint(equalToConstant: size)
            ])
            sparkles.append(Sparkle(view: sparkle, horizontal: horizontal, vertical: vertical, positions: positions[index]))
        }
        for name in [UIAccessibility.reduceMotionStatusDidChangeNotification, UIApplication.didEnterBackgroundNotification, UIApplication.didBecomeActiveNotification] {
            NotificationCenter.default.addObserver(self, selector: #selector(updateAnimations), name: name, object: nil)
        }
    }

    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }

    deinit { NotificationCenter.default.removeObserver(self) }

    override func layoutSubviews() {
        super.layoutSubviews()
        CATransaction.begin()
        CATransaction.setDisableActions(true)
        glow.frame = glowView.bounds
        CATransaction.commit()
    }

    override func didMoveToWindow() {
        super.didMoveToWindow()
        if window == nil {
            updateAnimations()
        } else {
            DispatchQueue.main.async { [weak self] in self?.updateAnimations() }
        }
    }

    @objc private func updateAnimations() {
        animationGeneration += 1
        glow.removeAllAnimations()
        for sparkle in sparkles {
            sparkle.view.layer.removeAllAnimations()
            sparkle.view.alpha = 0.85
            sparkle.view.transform = .identity
        }
        guard window != nil, UIApplication.shared.applicationState != .background,
              !UIAccessibility.isReduceMotionEnabled, UIView.areAnimationsEnabled else { return }
        let pulse = CABasicAnimation(keyPath: "opacity")
        pulse.fromValue = 0.6
        pulse.toValue = 1
        pulse.duration = 2.4
        pulse.autoreverses = true
        pulse.repeatCount = .infinity
        pulse.timingFunction = CAMediaTimingFunction(name: .easeInEaseOut)
        glow.add(pulse, forKey: "crownGlow")
        for index in sparkles.indices {
            animateSparkle(at: index, generation: animationGeneration, delay: Double(index) * 0.7)
        }
    }

    private func animateSparkle(at index: Int, generation: Int, delay: TimeInterval) {
        guard generation == animationGeneration, window != nil,
              UIApplication.shared.applicationState != .background,
              !UIAccessibility.isReduceMotionEnabled, UIView.areAnimationsEnabled else { return }
        let choices = sparkles[index].positions.indices.filter { $0 != sparkles[index].position }
        guard let next = choices.randomElement() else { return }
        sparkles[index].position = next
        let sparkle = sparkles[index]
        UIView.performWithoutAnimation {
            sparkle.view.alpha = 0
            sparkle.view.transform = CGAffineTransform(scaleX: 0.8, y: 0.8)
            sparkle.horizontal.constant = sparkle.positions[next].x
            sparkle.vertical.constant = sparkle.positions[next].y
            layoutIfNeeded()
        }
        UIView.animateKeyframes(withDuration: .random(in: 3...3.8), delay: delay, options: [.calculationModeCubic, .allowUserInteraction]) {
            UIView.addKeyframe(withRelativeStartTime: 0, relativeDuration: 0.45) {
                sparkle.view.alpha = 1
                sparkle.view.transform = CGAffineTransform(scaleX: 1.14, y: 1.14)
            }
            UIView.addKeyframe(withRelativeStartTime: 0.45, relativeDuration: 0.55) {
                sparkle.view.alpha = 0
                sparkle.view.transform = CGAffineTransform(scaleX: 0.8, y: 0.8)
            }
        } completion: { [weak self] finished in
            guard finished else { return }
            self?.animateSparkle(at: index, generation: generation, delay: .random(in: 0.15...0.45))
        }
    }
}

private final class WatermelonCrownSparkleView: UIView {
    override class var layerClass: AnyClass { CAShapeLayer.self }

    override init(frame: CGRect) {
        super.init(frame: frame)
        let shape = layer as! CAShapeLayer
        if #available(iOS 26.0, *) {
            shape.preferredDynamicRange = .high
            shape.fillColor = UIColor(red: 1, green: 0.94, blue: 0.72, alpha: 1, linearExposure: 2.2).cgColor
        } else {
            shape.fillColor = UIColor.Material.Amber._50.cgColor
        }
        shape.shadowColor = UIColor.Material.Amber._400.cgColor
        shape.shadowOpacity = 0.9
        shape.shadowRadius = 3
        shape.shadowOffset = .zero
    }

    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }

    override func layoutSubviews() {
        super.layoutSubviews()
        let center = CGPoint(x: bounds.midX, y: bounds.midY)
        let path = UIBezierPath()
        path.move(to: CGPoint(x: center.x, y: 0))
        path.addQuadCurve(to: CGPoint(x: bounds.maxX, y: center.y), controlPoint: center)
        path.addQuadCurve(to: CGPoint(x: center.x, y: bounds.maxY), controlPoint: center)
        path.addQuadCurve(to: CGPoint(x: 0, y: center.y), controlPoint: center)
        path.addQuadCurve(to: CGPoint(x: center.x, y: 0), controlPoint: center)
        path.close()
        (layer as! CAShapeLayer).path = path.cgPath
    }
}

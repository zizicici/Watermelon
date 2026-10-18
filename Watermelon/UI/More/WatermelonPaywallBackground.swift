import UIKit

enum WatermelonPaywallPalette {
    static let card = UIColor.materialAdaptive(light: .white.withAlphaComponent(0.72), dark: .white.withAlphaComponent(0.07))
    static let selectedCard = UIColor.materialAdaptive(light: .Material.Green._100.withAlphaComponent(0.9), dark: .Material.Green._900.withAlphaComponent(0.7))
    static let tableRow = UIColor.materialAdaptive(light: .white.withAlphaComponent(0.92), dark: .materialDarkSurface(tint: .Material.Green._200, alpha: 0.12))
    static let freeColumn = UIColor.appTint.withAlphaComponent(0.07)
    static let proColumn = UIColor.appTint.withAlphaComponent(0.14)
}

final class WatermelonPaywallBackground: UIView {
    private let base = CAGradientLayer()
    private let highlight = CAGradientLayer()
    private let green = CAGradientLayer()

    override init(frame: CGRect) {
        super.init(frame: frame)
        isUserInteractionEnabled = false
        clipsToBounds = true
        base.startPoint = CGPoint(x: 0, y: 0)
        base.endPoint = CGPoint(x: 1, y: 1)
        base.locations = [0, 0.5, 1]
        for glow in [highlight, green] {
            glow.type = .radial
            glow.startPoint = CGPoint(x: 0.5, y: 0.5)
            glow.endPoint = CGPoint(x: 1, y: 1)
        }
        for gradient in [base, highlight, green] { layer.addSublayer(gradient) }
        registerForTraitChanges([UITraitUserInterfaceStyle.self]) { (view: WatermelonPaywallBackground, _) in
            view.updateColors()
        }
        updateColors()
    }

    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }

    override func layoutSubviews() {
        super.layoutSubviews()
        CATransaction.begin()
        CATransaction.setDisableActions(true)
        base.frame = bounds
        highlight.frame = CGRect(x: -bounds.width * 0.7, y: -bounds.width * 0.5, width: bounds.width * 1.7, height: bounds.width * 1.7)
        green.frame = CGRect(x: bounds.width * 0.15, y: bounds.height * 0.28, width: bounds.width * 1.5, height: bounds.width * 1.5)
        CATransaction.commit()
    }

    private func updateColors() {
        let dark = traitCollection.userInterfaceStyle == .dark
        let colors: [UIColor] = dark
            ? [color(0x102B20), color(0x183D2A), color(0x1D2E1F)]
            : [color(0xC6E7C8), color(0xE2F1D3), color(0xF1F7E7)]
        base.colors = colors.map(\.cgColor)
        let highlightColor = dark ? color(0x6C9F50).withAlphaComponent(0.3) : color(0xB7E298).withAlphaComponent(0.6)
        highlight.colors = [highlightColor.cgColor, highlightColor.withAlphaComponent(0).cgColor]
        let greenColor = dark ? color(0x3E895E).withAlphaComponent(0.3) : color(0x52A971).withAlphaComponent(0.25)
        green.colors = [greenColor.cgColor, greenColor.withAlphaComponent(0).cgColor]
    }

    private func color(_ hex: UInt32) -> UIColor {
        UIColor(red: CGFloat((hex >> 16) & 0xFF) / 255, green: CGFloat((hex >> 8) & 0xFF) / 255, blue: CGFloat(hex & 0xFF) / 255, alpha: 1)
    }
}

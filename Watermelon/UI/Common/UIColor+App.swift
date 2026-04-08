import UIKit

extension UIColor {
    static let appPaper = UIColor(named: "PaperColor")!
    static let appText = UIColor(named: "TextColor")!
    static let appBackground = UIColor(named: "BackgroundColor")!
}

// MARK: - Material Adaptive Colors
//
// Naming follows M3 color roles. Tone values in M2 shade ≈ M3 tone:
//   _900≈10  _800≈20  _700≈30  _600≈40  _400≈60  _200≈80  _100≈90  _50≈95
//
// M3 baseline:
//   Light — primary: tone40(_600), onPrimary: white, container: tone90(_100), onContainer: tone10(_900)
//   Dark  — primary: tone80(_200), onPrimary: tone20(_800), container: tone30(_700), onContainer: tone90(_100)

extension UIColor {
    /// M3 adaptive color: returns `light` in light mode, `dark` in dark mode.
    static func materialAdaptive(light: UIColor, dark: UIColor) -> UIColor {
        UIColor { $0.userInterfaceStyle == .dark ? dark : light }
    }

    /// Primary role — used for icons, labels, accent elements.
    /// M3: light tone40(`_600`), dark tone80(`_200`).
    static func materialPrimary(light: UIColor, dark: UIColor) -> UIColor {
        materialAdaptive(light: light, dark: dark)
    }

    /// onPrimary role — text/icon on primary-colored background.
    /// M3: light white, dark tone20(`_800`).
    static func materialOnPrimary(lightColor: UIColor = .white, dark: UIColor) -> UIColor {
        materialAdaptive(light: lightColor, dark: dark)
    }

    /// onPrimaryContainer role — prominent text on a tinted surface.
    /// M3: light tone10(`_900`), dark tone90(`_100`).
    static func materialOnContainer(light: UIColor, dark: UIColor) -> UIColor {
        materialAdaptive(light: light, dark: dark)
    }

    /// onSurfaceVariant role — secondary/detail text on a surface.
    /// M3: light tone30(`_700`), dark tone80(`_200`).
    static func materialOnSurfaceVariant(light: UIColor, dark: UIColor) -> UIColor {
        materialAdaptive(light: light, dark: dark)
    }

    /// Tinted surface / container background.
    /// M3: light tone90-95(`_50`/`_100`), dark tinted surface.
    static func materialSurface(light: UIColor, darkTint: UIColor, darkAlpha: CGFloat = 0.08) -> UIColor {
        UIColor { $0.userInterfaceStyle == .dark ? .materialDarkSurface(tint: darkTint, alpha: darkAlpha) : light }
    }
}

// MARK: - Material Design Colors

extension UIColor {
    fileprivate convenience init(hex: UInt32) {
        self.init(
            red:   CGFloat((hex >> 16) & 0xFF) / 255.0,
            green: CGFloat((hex >> 8)  & 0xFF) / 255.0,
            blue:  CGFloat( hex        & 0xFF) / 255.0,
            alpha: 1.0
        )
    }

    /// Material dark theme surface (#121212) with a tinted color overlay.
    /// `tint` is the accent color, `alpha` controls the overlay strength (Material default: 0.08 for cards).
    static func materialDarkSurface(tint: UIColor, alpha: CGFloat = 0.08) -> UIColor {
        let base = UIColor(hex: 0x121212)
        var br: CGFloat = 0, bg: CGFloat = 0, bb: CGFloat = 0, ba: CGFloat = 0
        var tr: CGFloat = 0, tg: CGFloat = 0, tb: CGFloat = 0, ta: CGFloat = 0
        base.getRed(&br, green: &bg, blue: &bb, alpha: &ba)
        tint.getRed(&tr, green: &tg, blue: &tb, alpha: &ta)
        return UIColor(
            red:   br * (1 - alpha) + tr * alpha,
            green: bg * (1 - alpha) + tg * alpha,
            blue:  bb * (1 - alpha) + tb * alpha,
            alpha: 1.0
        )
    }

    enum Material {
        enum Red {
            static let _50   = UIColor(hex: 0xFFEBEE)
            static let _100  = UIColor(hex: 0xFFCDD2)
            static let _200  = UIColor(hex: 0xEF9A9A)
            static let _300  = UIColor(hex: 0xE57373)
            static let _400  = UIColor(hex: 0xEF5350)
            static let _500  = UIColor(hex: 0xF44336)
            static let _600  = UIColor(hex: 0xE53935)
            static let _700  = UIColor(hex: 0xD32F2F)
            static let _800  = UIColor(hex: 0xC62828)
            static let _900  = UIColor(hex: 0xB71C1C)
            static let a100  = UIColor(hex: 0xFF8A80)
            static let a200  = UIColor(hex: 0xFF5252)
            static let a400  = UIColor(hex: 0xFF1744)
            static let a700  = UIColor(hex: 0xD50000)
        }

        enum Pink {
            static let _50   = UIColor(hex: 0xFCE4EC)
            static let _100  = UIColor(hex: 0xF8BBD0)
            static let _200  = UIColor(hex: 0xF48FB1)
            static let _300  = UIColor(hex: 0xF06292)
            static let _400  = UIColor(hex: 0xEC407A)
            static let _500  = UIColor(hex: 0xE91E63)
            static let _600  = UIColor(hex: 0xD81B60)
            static let _700  = UIColor(hex: 0xC2185B)
            static let _800  = UIColor(hex: 0xAD1457)
            static let _900  = UIColor(hex: 0x880E4F)
            static let a100  = UIColor(hex: 0xFF80AB)
            static let a200  = UIColor(hex: 0xFF4081)
            static let a400  = UIColor(hex: 0xF50057)
            static let a700  = UIColor(hex: 0xC51162)
        }

        enum Purple {
            static let _50   = UIColor(hex: 0xF3E5F5)
            static let _100  = UIColor(hex: 0xE1BEE7)
            static let _200  = UIColor(hex: 0xCE93D8)
            static let _300  = UIColor(hex: 0xBA68C8)
            static let _400  = UIColor(hex: 0xAB47BC)
            static let _500  = UIColor(hex: 0x9C27B0)
            static let _600  = UIColor(hex: 0x8E24AA)
            static let _700  = UIColor(hex: 0x7B1FA2)
            static let _800  = UIColor(hex: 0x6A1B9A)
            static let _900  = UIColor(hex: 0x4A148C)
            static let a100  = UIColor(hex: 0xEA80FC)
            static let a200  = UIColor(hex: 0xE040FB)
            static let a400  = UIColor(hex: 0xD500F9)
            static let a700  = UIColor(hex: 0xAA00FF)
        }

        enum DeepPurple {
            static let _50   = UIColor(hex: 0xEDE7F6)
            static let _100  = UIColor(hex: 0xD1C4E9)
            static let _200  = UIColor(hex: 0xB39DDB)
            static let _300  = UIColor(hex: 0x9575CD)
            static let _400  = UIColor(hex: 0x7E57C2)
            static let _500  = UIColor(hex: 0x673AB7)
            static let _600  = UIColor(hex: 0x5E35B1)
            static let _700  = UIColor(hex: 0x512DA8)
            static let _800  = UIColor(hex: 0x4527A0)
            static let _900  = UIColor(hex: 0x311B92)
            static let a100  = UIColor(hex: 0xB388FF)
            static let a200  = UIColor(hex: 0x7C4DFF)
            static let a400  = UIColor(hex: 0x651FFF)
            static let a700  = UIColor(hex: 0x6200EA)
        }

        enum Indigo {
            static let _50   = UIColor(hex: 0xE8EAF6)
            static let _100  = UIColor(hex: 0xC5CAE9)
            static let _200  = UIColor(hex: 0x9FA8DA)
            static let _300  = UIColor(hex: 0x7986CB)
            static let _400  = UIColor(hex: 0x5C6BC0)
            static let _500  = UIColor(hex: 0x3F51B5)
            static let _600  = UIColor(hex: 0x3949AB)
            static let _700  = UIColor(hex: 0x303F9F)
            static let _800  = UIColor(hex: 0x283593)
            static let _900  = UIColor(hex: 0x1A237E)
            static let a100  = UIColor(hex: 0x8C9EFF)
            static let a200  = UIColor(hex: 0x536DFE)
            static let a400  = UIColor(hex: 0x3D5AFE)
            static let a700  = UIColor(hex: 0x304FFE)
        }

        enum Blue {
            static let _50   = UIColor(hex: 0xE3F2FD)
            static let _100  = UIColor(hex: 0xBBDEFB)
            static let _200  = UIColor(hex: 0x90CAF9)
            static let _300  = UIColor(hex: 0x64B5F6)
            static let _400  = UIColor(hex: 0x42A5F5)
            static let _500  = UIColor(hex: 0x2196F3)
            static let _600  = UIColor(hex: 0x1E88E5)
            static let _700  = UIColor(hex: 0x1976D2)
            static let _800  = UIColor(hex: 0x1565C0)
            static let _900  = UIColor(hex: 0x0D47A1)
            static let a100  = UIColor(hex: 0x82B1FF)
            static let a200  = UIColor(hex: 0x448AFF)
            static let a400  = UIColor(hex: 0x2979FF)
            static let a700  = UIColor(hex: 0x2962FF)
        }

        enum LightBlue {
            static let _50   = UIColor(hex: 0xE1F5FE)
            static let _100  = UIColor(hex: 0xB3E5FC)
            static let _200  = UIColor(hex: 0x81D4FA)
            static let _300  = UIColor(hex: 0x4FC3F7)
            static let _400  = UIColor(hex: 0x29B6F6)
            static let _500  = UIColor(hex: 0x03A9F4)
            static let _600  = UIColor(hex: 0x039BE5)
            static let _700  = UIColor(hex: 0x0288D1)
            static let _800  = UIColor(hex: 0x0277BD)
            static let _900  = UIColor(hex: 0x01579B)
            static let a100  = UIColor(hex: 0x80D8FF)
            static let a200  = UIColor(hex: 0x40C4FF)
            static let a400  = UIColor(hex: 0x00B0FF)
            static let a700  = UIColor(hex: 0x0091EA)
        }

        enum Cyan {
            static let _50   = UIColor(hex: 0xE0F7FA)
            static let _100  = UIColor(hex: 0xB2EBF2)
            static let _200  = UIColor(hex: 0x80DEEA)
            static let _300  = UIColor(hex: 0x4DD0E1)
            static let _400  = UIColor(hex: 0x26C6DA)
            static let _500  = UIColor(hex: 0x00BCD4)
            static let _600  = UIColor(hex: 0x00ACC1)
            static let _700  = UIColor(hex: 0x0097A7)
            static let _800  = UIColor(hex: 0x00838F)
            static let _900  = UIColor(hex: 0x006064)
            static let a100  = UIColor(hex: 0x84FFFF)
            static let a200  = UIColor(hex: 0x18FFFF)
            static let a400  = UIColor(hex: 0x00E5FF)
            static let a700  = UIColor(hex: 0x00B8D4)
        }

        enum Teal {
            static let _50   = UIColor(hex: 0xE0F2F1)
            static let _100  = UIColor(hex: 0xB2DFDB)
            static let _200  = UIColor(hex: 0x80CBC4)
            static let _300  = UIColor(hex: 0x4DB6AC)
            static let _400  = UIColor(hex: 0x26A69A)
            static let _500  = UIColor(hex: 0x009688)
            static let _600  = UIColor(hex: 0x00897B)
            static let _700  = UIColor(hex: 0x00796B)
            static let _800  = UIColor(hex: 0x00695C)
            static let _900  = UIColor(hex: 0x004D40)
            static let a100  = UIColor(hex: 0xA7FFEB)
            static let a200  = UIColor(hex: 0x64FFDA)
            static let a400  = UIColor(hex: 0x1DE9B6)
            static let a700  = UIColor(hex: 0x00BFA5)
        }

        enum Green {
            static let _50   = UIColor(hex: 0xE8F5E9)
            static let _100  = UIColor(hex: 0xC8E6C9)
            static let _200  = UIColor(hex: 0xA5D6A7)
            static let _300  = UIColor(hex: 0x81C784)
            static let _400  = UIColor(hex: 0x66BB6A)
            static let _500  = UIColor(hex: 0x4CAF50)
            static let _600  = UIColor(hex: 0x43A047)
            static let _700  = UIColor(hex: 0x388E3C)
            static let _800  = UIColor(hex: 0x2E7D32)
            static let _900  = UIColor(hex: 0x1B5E20)
            static let a100  = UIColor(hex: 0xB9F6CA)
            static let a200  = UIColor(hex: 0x69F0AE)
            static let a400  = UIColor(hex: 0x00E676)
            static let a700  = UIColor(hex: 0x00C853)
        }

        enum LightGreen {
            static let _50   = UIColor(hex: 0xF1F8E9)
            static let _100  = UIColor(hex: 0xDCEDC8)
            static let _200  = UIColor(hex: 0xC5E1A5)
            static let _300  = UIColor(hex: 0xAED581)
            static let _400  = UIColor(hex: 0x9CCC65)
            static let _500  = UIColor(hex: 0x8BC34A)
            static let _600  = UIColor(hex: 0x7CB342)
            static let _700  = UIColor(hex: 0x689F38)
            static let _800  = UIColor(hex: 0x558B2F)
            static let _900  = UIColor(hex: 0x33691E)
            static let a100  = UIColor(hex: 0xCCFF90)
            static let a200  = UIColor(hex: 0xB2FF59)
            static let a400  = UIColor(hex: 0x76FF03)
            static let a700  = UIColor(hex: 0x64DD17)
        }

        enum Lime {
            static let _50   = UIColor(hex: 0xF9FBE7)
            static let _100  = UIColor(hex: 0xF0F4C3)
            static let _200  = UIColor(hex: 0xE6EE9C)
            static let _300  = UIColor(hex: 0xDCE775)
            static let _400  = UIColor(hex: 0xD4E157)
            static let _500  = UIColor(hex: 0xCDDC39)
            static let _600  = UIColor(hex: 0xC0CA33)
            static let _700  = UIColor(hex: 0xAFB42B)
            static let _800  = UIColor(hex: 0x9E9D24)
            static let _900  = UIColor(hex: 0x827717)
            static let a100  = UIColor(hex: 0xF4FF81)
            static let a200  = UIColor(hex: 0xEEFF41)
            static let a400  = UIColor(hex: 0xC6FF00)
            static let a700  = UIColor(hex: 0xAEEA00)
        }

        enum Yellow {
            static let _50   = UIColor(hex: 0xFFFDE7)
            static let _100  = UIColor(hex: 0xFFF9C4)
            static let _200  = UIColor(hex: 0xFFF59D)
            static let _300  = UIColor(hex: 0xFFF176)
            static let _400  = UIColor(hex: 0xFFEE58)
            static let _500  = UIColor(hex: 0xFFEB3B)
            static let _600  = UIColor(hex: 0xFDD835)
            static let _700  = UIColor(hex: 0xFBC02D)
            static let _800  = UIColor(hex: 0xF9A825)
            static let _900  = UIColor(hex: 0xF57F17)
            static let a100  = UIColor(hex: 0xFFFF8D)
            static let a200  = UIColor(hex: 0xFFFF00)
            static let a400  = UIColor(hex: 0xFFEA00)
            static let a700  = UIColor(hex: 0xFFD600)
        }

        enum Amber {
            static let _50   = UIColor(hex: 0xFFF8E1)
            static let _100  = UIColor(hex: 0xFFECB3)
            static let _200  = UIColor(hex: 0xFFE082)
            static let _300  = UIColor(hex: 0xFFD54F)
            static let _400  = UIColor(hex: 0xFFCA28)
            static let _500  = UIColor(hex: 0xFFC107)
            static let _600  = UIColor(hex: 0xFFB300)
            static let _700  = UIColor(hex: 0xFFA000)
            static let _800  = UIColor(hex: 0xFF8F00)
            static let _900  = UIColor(hex: 0xFF6F00)
            static let a100  = UIColor(hex: 0xFFE57F)
            static let a200  = UIColor(hex: 0xFFD740)
            static let a400  = UIColor(hex: 0xFFC400)
            static let a700  = UIColor(hex: 0xFFAB00)
        }

        enum Orange {
            static let _50   = UIColor(hex: 0xFFF3E0)
            static let _100  = UIColor(hex: 0xFFE0B2)
            static let _200  = UIColor(hex: 0xFFCC80)
            static let _300  = UIColor(hex: 0xFFB74D)
            static let _400  = UIColor(hex: 0xFFA726)
            static let _500  = UIColor(hex: 0xFF9800)
            static let _600  = UIColor(hex: 0xFB8C00)
            static let _700  = UIColor(hex: 0xF57C00)
            static let _800  = UIColor(hex: 0xEF6C00)
            static let _900  = UIColor(hex: 0xE65100)
            static let a100  = UIColor(hex: 0xFFD180)
            static let a200  = UIColor(hex: 0xFFAB40)
            static let a400  = UIColor(hex: 0xFF9100)
            static let a700  = UIColor(hex: 0xFF6D00)
        }

        enum DeepOrange {
            static let _50   = UIColor(hex: 0xFBE9E7)
            static let _100  = UIColor(hex: 0xFFCCBC)
            static let _200  = UIColor(hex: 0xFFAB91)
            static let _300  = UIColor(hex: 0xFF8A65)
            static let _400  = UIColor(hex: 0xFF7043)
            static let _500  = UIColor(hex: 0xFF5722)
            static let _600  = UIColor(hex: 0xF4511E)
            static let _700  = UIColor(hex: 0xE64A19)
            static let _800  = UIColor(hex: 0xD84315)
            static let _900  = UIColor(hex: 0xBF360C)
            static let a100  = UIColor(hex: 0xFF9E80)
            static let a200  = UIColor(hex: 0xFF6E40)
            static let a400  = UIColor(hex: 0xFF3D00)
            static let a700  = UIColor(hex: 0xDD2C00)
        }

        enum Brown {
            static let _50   = UIColor(hex: 0xEFEBE9)
            static let _100  = UIColor(hex: 0xD7CCC8)
            static let _200  = UIColor(hex: 0xBCAAA4)
            static let _300  = UIColor(hex: 0xA1887F)
            static let _400  = UIColor(hex: 0x8D6E63)
            static let _500  = UIColor(hex: 0x795548)
            static let _600  = UIColor(hex: 0x6D4C41)
            static let _700  = UIColor(hex: 0x5D4037)
            static let _800  = UIColor(hex: 0x4E342E)
            static let _900  = UIColor(hex: 0x3E2723)
        }

        enum Grey {
            static let _50   = UIColor(hex: 0xFAFAFA)
            static let _100  = UIColor(hex: 0xF5F5F5)
            static let _200  = UIColor(hex: 0xEEEEEE)
            static let _300  = UIColor(hex: 0xE0E0E0)
            static let _400  = UIColor(hex: 0xBDBDBD)
            static let _500  = UIColor(hex: 0x9E9E9E)
            static let _600  = UIColor(hex: 0x757575)
            static let _700  = UIColor(hex: 0x616161)
            static let _800  = UIColor(hex: 0x424242)
            static let _900  = UIColor(hex: 0x212121)
        }

        enum BlueGrey {
            static let _50   = UIColor(hex: 0xECEFF1)
            static let _100  = UIColor(hex: 0xCFD8DC)
            static let _200  = UIColor(hex: 0xB0BEC5)
            static let _300  = UIColor(hex: 0x90A4AE)
            static let _400  = UIColor(hex: 0x78909C)
            static let _500  = UIColor(hex: 0x607D8B)
            static let _600  = UIColor(hex: 0x546E7A)
            static let _700  = UIColor(hex: 0x455A64)
            static let _800  = UIColor(hex: 0x37474F)
            static let _900  = UIColor(hex: 0x263238)
        }
    }
}

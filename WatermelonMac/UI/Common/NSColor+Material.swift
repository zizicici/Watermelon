import AppKit

extension NSColor {
    static let wmMaterialPrimary = wmAdaptive(
        name: "wm.material.primary",
        light: NSColor(hex: 0x43A047),
        dark: NSColor(hex: 0xA5D6A7)
    )
    static let wmMaterialPrimaryContainer = wmAdaptive(
        name: "wm.material.primaryContainer",
        light: NSColor(hex: 0xC8E6C9),
        dark: NSColor(hex: 0x2E7D32)
    )
    static let wmMaterialOnPrimaryContainer = wmAdaptive(
        name: "wm.material.onPrimaryContainer",
        light: NSColor(hex: 0x1B5E20),
        dark: NSColor(hex: 0xC8E6C9)
    )
    static let wmMaterialPrimaryDetail = wmAdaptive(
        name: "wm.material.primaryDetail",
        light: NSColor(hex: 0x388E3C),
        dark: NSColor(hex: 0xA5D6A7)
    )
    static let wmMaterialPrimarySurface = wmAdaptive(
        name: "wm.material.primarySurface",
        light: NSColor(hex: 0xE8F5E9),
        dark: NSColor(hex: 0x182A19)
    )
    static let wmMaterialWarningContainer = wmAdaptive(
        name: "wm.material.warningContainer",
        light: NSColor(hex: 0xFFECB3),
        dark: NSColor(hex: 0xFF8F00)
    )
    static let wmMaterialOnWarningContainer = wmAdaptive(
        name: "wm.material.onWarningContainer",
        light: NSColor(hex: 0xFF6F00),
        dark: NSColor(hex: 0xFFECB3)
    )
    static let wmMaterialWarningDetail = wmAdaptive(
        name: "wm.material.warningDetail",
        light: NSColor(hex: 0xFF8F00),
        dark: NSColor(hex: 0xFFE082)
    )
    static let wmMaterialError = wmAdaptive(
        name: "wm.material.error",
        light: NSColor(hex: 0xE53935),
        dark: NSColor(hex: 0xEF9A9A)
    )
    static let wmMaterialBackup = wmAdaptive(
        name: "wm.material.backup",
        light: NSColor(hex: 0x00ACC1),
        dark: NSColor(hex: 0x80DEEA)
    )
    static let wmMaterialDownload = wmAdaptive(
        name: "wm.material.download",
        light: NSColor(hex: 0xFB8C00),
        dark: NSColor(hex: 0xFFCC80)
    )
    static let wmMaterialComplement = wmAdaptive(
        name: "wm.material.complement",
        light: NSColor(hex: 0x8E24AA),
        dark: NSColor(hex: 0xCE93D8)
    )
    static let wmMaterialPhotoSurfaces = [
        NSColor(hex: 0xC8E6C9),
        NSColor(hex: 0xB2DFDB),
        NSColor(hex: 0xBBDEFB),
        NSColor(hex: 0xFFE0B2),
        NSColor(hex: 0xF8BBD0),
    ]
    private static let wmMaterialMonthSurfaces = [
        wmAdaptive(
            name: "wm.material.month.green.surface",
            light: NSColor(hex: 0xE8F5E9),
            dark: NSColor(hex: 0x182A19)
        ),
        wmAdaptive(
            name: "wm.material.month.blue.surface",
            light: NSColor(hex: 0xE3F2FD),
            dark: NSColor(hex: 0x15222C)
        ),
        wmAdaptive(
            name: "wm.material.month.amber.surface",
            light: NSColor(hex: 0xFFF8E1),
            dark: NSColor(hex: 0x2B2414)
        ),
        wmAdaptive(
            name: "wm.material.month.red.surface",
            light: NSColor(hex: 0xFFEBEE),
            dark: NSColor(hex: 0x2C191B)
        ),
    ]
    private static let wmMaterialMonthTitles = [
        wmAdaptive(
            name: "wm.material.month.green.title",
            light: NSColor(hex: 0x1B5E20),
            dark: NSColor(hex: 0xC8E6C9)
        ),
        wmAdaptive(
            name: "wm.material.month.blue.title",
            light: NSColor(hex: 0x0D47A1),
            dark: NSColor(hex: 0xBBDEFB)
        ),
        wmAdaptive(
            name: "wm.material.month.amber.title",
            light: NSColor(hex: 0xFF6F00),
            dark: NSColor(hex: 0xFFECB3)
        ),
        wmAdaptive(
            name: "wm.material.month.red.title",
            light: NSColor(hex: 0xB71C1C),
            dark: NSColor(hex: 0xFFCDD2)
        ),
    ]
    private static let wmMaterialMonthDetails = [
        wmAdaptive(
            name: "wm.material.month.green.detail",
            light: NSColor(hex: 0x388E3C),
            dark: NSColor(hex: 0xA5D6A7)
        ),
        wmAdaptive(
            name: "wm.material.month.blue.detail",
            light: NSColor(hex: 0x1976D2),
            dark: NSColor(hex: 0x90CAF9)
        ),
        wmAdaptive(
            name: "wm.material.month.amber.detail",
            light: NSColor(hex: 0xFFA000),
            dark: NSColor(hex: 0xFFE082)
        ),
        wmAdaptive(
            name: "wm.material.month.red.detail",
            light: NSColor(hex: 0xD32F2F),
            dark: NSColor(hex: 0xEF9A9A)
        ),
    ]

    static func wmMaterialMonthSurface(for month: Int) -> NSColor {
        wmMaterialMonthSurfaces[wmMaterialSeasonIndex(for: month)]
    }

    static func wmMaterialMonthTitle(for month: Int) -> NSColor {
        wmMaterialMonthTitles[wmMaterialSeasonIndex(for: month)]
    }

    static func wmMaterialMonthDetail(for month: Int) -> NSColor {
        wmMaterialMonthDetails[wmMaterialSeasonIndex(for: month)]
    }

    private convenience init(hex: UInt32) {
        self.init(
            srgbRed: CGFloat((hex >> 16) & 0xFF) / 255,
            green: CGFloat((hex >> 8) & 0xFF) / 255,
            blue: CGFloat(hex & 0xFF) / 255,
            alpha: 1
        )
    }

    private static func wmAdaptive(
        name: String,
        light: NSColor,
        dark: NSColor
    ) -> NSColor {
        NSColor(name: NSColor.Name(name)) { appearance in
            appearance.bestMatch(
                from: [.darkAqua, .aqua]
            ) == .darkAqua ? dark : light
        }
    }

    private static func wmMaterialSeasonIndex(for month: Int) -> Int {
        switch month {
        case 1...3:
            return 0
        case 4...6:
            return 1
        case 7...9:
            return 2
        case 10...12:
            return 3
        default:
            return 0
        }
    }
}

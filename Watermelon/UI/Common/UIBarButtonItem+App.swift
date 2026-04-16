import UIKit

extension UIBarButtonItem.Style {
    static var prominentStyle: UIBarButtonItem.Style {
        if #available(iOS 26.0, *) { return .prominent }
        return .plain
    }
}

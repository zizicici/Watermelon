import UIKit

enum MediaPresenceStyle {
    static func image(for presence: MediaPresence, remoteImage: UIImage?) -> UIImage? {
        switch presence {
        case .localOnly: UIImage(systemName: "iphone")
        case .remoteOnly: remoteImage
        case .both: UIImage(systemName: "arrow.trianglehead.2.clockwise.rotate.90")
        }
    }
}

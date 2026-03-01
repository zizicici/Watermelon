//
//  UIViewController+Extension.swift
//  coco
//
//  Created by zici on 2023/5/11.
//

import UIKit
import SafariServices

extension UIViewController {
    func openSF(with url: URL) {
        let safariViewController = SFSafariViewController(url: url)
        navigationController?.present(safariViewController, animated: ConsideringUser.animated)
    }
}

extension UIViewController {
    func showAlert(title: String?, message: String?) {
        let alertController = UIAlertController(title: title, message: message, preferredStyle: .alert)
        let cancelAction = UIAlertAction(title: String(localized: "button.ok"), style: .cancel)
        alertController.addAction(cancelAction)

        present(alertController, animated: ConsideringUser.animated, completion: nil)
    }
}

extension UIViewController {
    func jumpToSettings() {
        guard let url = URL(string: UIApplication.openSettingsURLString) else {
            return
        }
        if UIApplication.shared.canOpenURL(url) {
            UIApplication.shared.open(url, options: [:])
        }
    }
}

import AVFoundation
import UIKit

// Endpoints for the media viewer's zoom (hero) transition. Frames are exchanged in window coordinates, so
// they stay valid whether the grid is pushed full-screen or presented inside a sheet.
protocol HeroTransitionSource: AnyObject {
    // The grid thumbnail's image + on-screen frame for an item, or nil when its cell isn't on screen.
    func heroSource(forItemID id: String) -> (image: UIImage, frameInWindow: CGRect)?
    // The grid cell's on-screen frame only — resolved from layout attributes, so it works even when the
    // cell isn't currently dequeued or its thumbnail hasn't loaded (the dismiss target only needs a frame).
    func heroSourceFrame(forItemID id: String) -> CGRect?
    func heroPrepareSource(forItemID id: String, hidden: Bool)
    func heroScrollToItem(id: String)
}

protocol HeroTransitionDestination: AnyObject {
    var heroCurrentItemID: String { get }
    // The currently displayed image + its on-screen (aspect-fit) frame, or nil (e.g. not yet loaded / live).
    func heroDestination() -> (image: UIImage, frameInWindow: CGRect)?
    func heroPrepareDestination(hidden: Bool)
}

// Drives the non-interactive paths: present (tap a grid cell → zoom up) and tap-close (zoom back). The
// interactive drag-dismiss is handled manually in the viewer.
final class HeroTransition: NSObject, UIViewControllerTransitioningDelegate {
    weak var source: HeroTransitionSource?
    weak var destination: HeroTransitionDestination?
    // The item the viewer was opened at (present zoom origin). Dismiss uses destination.heroCurrentItemID.
    var presentItemID: String?

    func animationController(forPresented presented: UIViewController, presenting: UIViewController, source: UIViewController) -> UIViewControllerAnimatedTransitioning? {
        HeroZoomAnimator(isPresenting: true, transition: self)
    }

    func animationController(forDismissed dismissed: UIViewController) -> UIViewControllerAnimatedTransitioning? {
        HeroZoomAnimator(isPresenting: false, transition: self)
    }
}

private final class HeroZoomAnimator: NSObject, UIViewControllerAnimatedTransitioning {
    private let isPresenting: Bool
    private let transition: HeroTransition
    private static let duration: TimeInterval = 0.25

    init(isPresenting: Bool, transition: HeroTransition) {
        self.isPresenting = isPresenting
        self.transition = transition
    }

    func transitionDuration(using ctx: UIViewControllerContextTransitioning?) -> TimeInterval { Self.duration }

    func animateTransition(using ctx: UIViewControllerContextTransitioning) {
        isPresenting ? animatePresent(ctx) : animateDismiss(ctx)
    }

    private func animatePresent(_ ctx: UIViewControllerContextTransitioning) {
        let container = ctx.containerView
        guard let toVC = ctx.viewController(forKey: .to) else { ctx.completeTransition(true); return }
        let toView = toVC.view!
        toView.frame = ctx.finalFrame(for: toVC)
        container.addSubview(toView)
        toView.layoutIfNeeded()

        guard let itemID = transition.presentItemID,
              let src = transition.source?.heroSource(forItemID: itemID) else {
            toView.alpha = 0
            UIView.animate(withDuration: Self.duration, animations: { toView.alpha = 1 }) { _ in
                ctx.completeTransition(!ctx.transitionWasCancelled)
            }
            return
        }

        let temp = UIImageView(image: src.image)
        temp.contentMode = .scaleAspectFill
        temp.clipsToBounds = true
        temp.frame = src.frameInWindow
        container.addSubview(temp)

        let backdrop = toView.backgroundColor
        toView.backgroundColor = backdrop?.withAlphaComponent(0)
        let hidden = toView.subviews.filter { $0 !== temp }
        hidden.forEach { $0.alpha = 0 }
        transition.source?.heroPrepareSource(forItemID: itemID, hidden: true)

        let endFrame = AVMakeRect(aspectRatio: src.image.size, insideRect: toView.bounds)
        UIView.animate(withDuration: Self.duration, delay: 0, options: [.curveEaseInOut], animations: {
            temp.frame = endFrame
            toView.backgroundColor = backdrop
        }) { _ in
            hidden.forEach { $0.alpha = 1 }
            temp.removeFromSuperview()
            self.transition.source?.heroPrepareSource(forItemID: itemID, hidden: false)
            ctx.completeTransition(!ctx.transitionWasCancelled)
        }
    }

    private func animateDismiss(_ ctx: UIViewControllerContextTransitioning) {
        let container = ctx.containerView
        guard let fromVC = ctx.viewController(forKey: .from) else { ctx.completeTransition(true); return }
        let fromView = fromVC.view!
        let itemID = transition.destination?.heroCurrentItemID ?? ""
        transition.source?.heroScrollToItem(id: itemID)

        guard let dst = transition.destination?.heroDestination(),
              let targetFrame = transition.source?.heroSourceFrame(forItemID: itemID) else {
            UIView.animate(withDuration: Self.duration, animations: { fromView.alpha = 0 }) { _ in
                fromView.removeFromSuperview()
                ctx.completeTransition(!ctx.transitionWasCancelled)
            }
            return
        }

        let temp = UIImageView(image: dst.image)
        temp.contentMode = .scaleAspectFill
        temp.clipsToBounds = true
        temp.frame = dst.frameInWindow
        container.addSubview(temp)

        fromView.subviews.forEach { $0.alpha = 0 }
        transition.source?.heroPrepareSource(forItemID: itemID, hidden: true)
        let backdrop = fromView.backgroundColor

        UIView.animate(withDuration: Self.duration, delay: 0, options: [.curveEaseInOut], animations: {
            temp.frame = targetFrame
            fromView.backgroundColor = backdrop?.withAlphaComponent(0)
        }) { _ in
            temp.removeFromSuperview()
            self.transition.source?.heroPrepareSource(forItemID: itemID, hidden: false)
            fromView.removeFromSuperview()
            ctx.completeTransition(!ctx.transitionWasCancelled)
        }
    }
}

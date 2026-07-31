enum MacPhotoBrowserPhotoAccessTransitionPolicy {
    static func invalidatesLocalProjection(
        previous: PhotoLibraryAccessState,
        current: PhotoLibraryAccessState
    ) -> Bool {
        previous.canReadLibrary != current.canReadLibrary
            || (
                previous.canReadLibrary
                    && current.canReadLibrary
                    && previous != current
            )
    }
}

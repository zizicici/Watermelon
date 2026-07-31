enum MacDemoPhotoLibraryPolicy {
    static func usesSyntheticLibrary(
        arguments: [String]
    ) -> Bool {
        #if DEBUG
        arguments.contains("--demo-photo-library")
            || arguments.contains("--demo-no-destination")
            || arguments.contains("--demo-connecting")
            || arguments.contains("--demo-connected")
        #else
        false
        #endif
    }
}

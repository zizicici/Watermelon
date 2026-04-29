import SwiftUI

struct RootView: View {
    @EnvironmentObject private var container: MacDependencyContainer
    @State private var selection: ServerProfileRecord.ID?

    var body: some View {
        NavigationSplitView {
            ProfileListView(
                store: container.profileStore,
                storageClientFactory: container.storageClientFactory,
                selection: $selection
            )
            .frame(minWidth: 240)
        } detail: {
            if let id = selection,
               let profile = container.profileStore.profiles.first(where: { $0.id == id }) {
                LegacyImportRootView(
                    profile: profile,
                    storageClientFactory: container.storageClientFactory,
                    profileStore: container.profileStore
                )
                .id(profile.id)
            } else {
                ContentUnavailableView(
                    String(localized: "profiles.detail.empty.title"),
                    systemImage: "externaldrive",
                    description: Text(String(localized: "profiles.detail.empty.message"))
                )
            }
        }
    }
}

#Preview {
    RootView()
        .environmentObject(MacDependencyContainer())
}

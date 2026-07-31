enum MacProfileConnectionEditorMode {
    case create
    case edit

    init(hasEditingProfile: Bool) {
        self = hasEditingProfile ? .edit : .create
    }

    var showsNameField: Bool {
        self == .create
    }
}

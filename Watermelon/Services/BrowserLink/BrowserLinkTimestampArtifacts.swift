import Foundation
import os

enum BrowserLinkTimestampArtifacts {
    static var folderName: String { folderName(languageIdentifier: currentLanguageIdentifier) }
    static let guideName = "Watermelon - Restore file dates.html"
    static let windowsScriptName = "Watermelon - Restore file dates (Windows).ps1"
    static let macScriptName = "Watermelon - Restore file dates (macOS).sh"

    static func installTools(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async -> Bool {
        let folderPath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: folderName)
        do {
            try await client.createDirectory(path: folderPath)
        } catch {
            browserLinkLog.error("Timestamp tool folder install skipped type=\(String(reflecting: type(of: error)), privacy: .public)")
            return false
        }
        let artifacts = [
            (guideData, guideName),
            (windowsScriptData, windowsScriptName),
            (macScriptData, macScriptName),
        ]
        var installed = true
        for (data, name) in artifacts {
            do {
                try await upload(
                    data,
                    client: client,
                    path: RemotePathBuilder.absolutePath(basePath: folderPath, remoteRelativePath: name)
                )
            } catch {
                if !remoteStorageIsNameCollision(error) {
                    installed = false
                    browserLinkLog.error("Timestamp guide install skipped type=\(String(reflecting: type(of: error)), privacy: .public)")
                }
            }
        }
        return installed
    }

    static var guideData: Data { guideData(languageIdentifier: currentLanguageIdentifier) }
    static func guideData(languageIdentifier: String) -> Data {
        Data(guide(languageIdentifier: languageIdentifier).utf8)
    }
    static func folderName(languageIdentifier: String) -> String {
        switch resolvedLanguage(languageIdentifier) {
        case "de": "So stellen Sie Dateiänderungsdaten wieder her"
        case "es", "es-419": "Cómo restaurar las fechas de modificación"
        case "fr": "Comment restaurer les dates de modification"
        case "ja": "ファイルの更新日時を復元する方法"
        case "ko": "파일 수정 날짜를 복원하는 방법"
        case "pt-BR", "pt-PT": "Como restaurar as datas de modificação"
        case "ru": "Как восстановить даты изменения файлов"
        case "uk": "Як відновити дати зміни файлів"
        case "zh-Hans": "如何恢复文件修改日期"
        case "zh-Hant": "如何還原檔案修改日期"
        default: "How to restore file modification dates"
        }
    }
    static var windowsScriptData: Data { windowsScriptData(languageIdentifier: currentLanguageIdentifier) }
    static var macScriptData: Data { macScriptData(languageIdentifier: currentLanguageIdentifier) }
    static func windowsScriptData(languageIdentifier: String) -> Data {
        Data(windowsScript(languageIdentifier: languageIdentifier).utf8)
    }
    static func macScriptData(languageIdentifier: String) -> Data {
        Data(macScript(languageIdentifier: languageIdentifier).utf8)
    }

    private static func upload(
        _ data: Data,
        client: any RemoteStorageClientProtocol,
        path: String
    ) async throws {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("browser-link-artifact-\(UUID().uuidString)")
        try data.write(to: url, options: .atomic)
        defer { try? FileManager.default.removeItem(at: url) }
        try await client.upload(
            localURL: url,
            remotePath: path,
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )
    }

    private struct GuideCopy {
        let languageTag: String
        let title: String
        let heading: String
        let intro: String
        let noteTitle: String
        let noteBody: String
        let wait: String
        let windowsIntro: String
        let macIntro: String
        let runLabel: String
        let footer: String
    }

    private static var currentLanguageIdentifier: String {
        Bundle.main.preferredLocalizations.first ?? Locale.preferredLanguages.first ?? "en"
    }

    private static func guide(languageIdentifier: String) -> String {
        let copy = guideCopy(languageIdentifier: languageIdentifier)
        let localizedFolderName = folderName(languageIdentifier: languageIdentifier)
        let windowsCommand = "powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File \".\\\(localizedFolderName)\\\(windowsScriptName)\" \".\""
        let macCommand = "zsh \"./\(localizedFolderName)/\(macScriptName)\" \".\""
        return #"""
    <!doctype html>
    <html lang="\#(htmlEscaped(copy.languageTag))">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>\#(htmlEscaped(copy.title))</title>
      <style>
        :root { color-scheme: light dark; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
        body { max-width: 760px; margin: 0 auto; padding: 48px 24px 80px; line-height: 1.6; }
        h1 { font-size: 2rem; line-height: 1.2; }
        h2 { margin-top: 2.2rem; }
        code { font-family: ui-monospace, SFMono-Regular, Consolas, monospace; }
        pre { overflow-x: auto; padding: 16px; border: 1px solid #8886; border-radius: 10px; }
        .note { padding: 16px; border-left: 4px solid #16835d; background: #16835d12; }
      </style>
    </head>
    <body>
      <h1>\#(htmlEscaped(copy.heading))</h1>
      <p>\#(htmlEscaped(copy.intro))</p>
      <div class="note"><strong>\#(htmlEscaped(copy.noteTitle))</strong> \#(htmlEscaped(copy.noteBody))</div>
      <p>\#(htmlEscaped(copy.wait))</p>
      <p>\#(htmlEscaped(progressNote(languageIdentifier: languageIdentifier)))</p>

      <h2>Windows</h2>
      <ol>
        <li>\#(htmlEscaped(copy.windowsIntro))</li>
        <li>\#(htmlEscaped(copy.runLabel))</li>
      </ol>
      <pre><code>\#(htmlEscaped(windowsCommand))</code></pre>

      <h2>macOS</h2>
      <ol>
        <li>\#(htmlEscaped(copy.macIntro))</li>
        <li>\#(htmlEscaped(copy.runLabel))</li>
      </ol>
      <pre><code>\#(htmlEscaped(macCommand))</code></pre>

      <p>\#(htmlEscaped(copy.footer))</p>
    </body>
    </html>
    """#
    }

    private static func guideCopy(languageIdentifier: String) -> GuideCopy {
        switch resolvedLanguage(languageIdentifier) {
        case "de":
            GuideCopy(languageTag: "de", title: "Dateidaten wiederherstellen — Watermelon Backup", heading: "Ursprüngliche Dateidaten wiederherstellen", intro: "Browser können das Änderungsdatum beim Speichern nicht setzen. Deshalb enthält Watermelon Backup zwei lesbare Skripte. Nichts wird automatisch ausgeführt.", noteTitle: "Was das Skript macht:", noteBody: "Es liest die vorhandenen Manifeste unter .watermelon/months/*.sqlite und ändert nur die Änderungsdaten passender Sicherungsdateien. Es verbindet sich nicht mit dem Internet und lädt nichts hoch, löscht, benennt oder verschiebt nichts.", wait: "Warten Sie, bis die Sicherung beendet und das iPhone getrennt ist. Sie können das Skript vor der Ausführung in einem Texteditor prüfen.", windowsIntro: "Öffnen Sie den Sicherungsordner in Windows Terminal oder PowerShell.", macIntro: "Öffnen Sie Terminal, geben Sie cd gefolgt von einem Leerzeichen ein, ziehen Sie den Sicherungsordner hinein und drücken Sie die Eingabetaste.", runLabel: "Führen Sie aus:", footer: "Die Skripte sind Klartext und nicht als ausführbar markiert. Jeder Befehl wird nur durch Ihre ausdrückliche einmalige Aktion ausgeführt. Unter macOS werden Änderungsdaten sekundengenau wiederhergestellt.")
        case "es":
            GuideCopy(languageTag: "es", title: "Restaurar fechas de archivo — Watermelon Backup", heading: "Restaurar las fechas originales", intro: "Los navegadores no pueden establecer la fecha de modificación al guardar. Por eso Watermelon Backup incluye dos scripts legibles. Nada se ejecuta automáticamente.", noteTitle: "Qué hace el script:", noteBody: "Lee los manifiestos existentes en .watermelon/months/*.sqlite y solo actualiza la fecha de modificación de los archivos de copia coincidentes. No se conecta a Internet ni sube, elimina, renombra o mueve archivos.", wait: "Espera a que termine la copia y el iPhone se desconecte. Puedes revisar el script con cualquier editor de texto antes de ejecutarlo.", windowsIntro: "Abre esta carpeta de copia en Windows Terminal o PowerShell.", macIntro: "Abre Terminal, escribe cd y un espacio, arrastra esta carpeta de copia a la ventana y pulsa Retorno.", runLabel: "Ejecuta:", footer: "Los scripts son texto sin formato y no están marcados como ejecutables. Cada comando requiere una acción explícita y única. En macOS las fechas se restauran con precisión de segundos.")
        case "es-419":
            GuideCopy(languageTag: "es-419", title: "Restaurar fechas de archivo — Watermelon Backup", heading: "Restaurar las fechas originales", intro: "Los navegadores no pueden establecer la fecha de modificación al guardar. Por eso Watermelon Backup incluye dos scripts legibles. Nada se ejecuta automáticamente.", noteTitle: "Qué hace el script:", noteBody: "Lee los manifiestos existentes en .watermelon/months/*.sqlite y solo actualiza la fecha de modificación de los archivos de respaldo coincidentes. No se conecta a Internet ni sube, elimina, renombra o mueve archivos.", wait: "Espera a que termine el respaldo y el iPhone se desconecte. Puedes revisar el script con cualquier editor de texto antes de ejecutarlo.", windowsIntro: "Abre esta carpeta de respaldo en Windows Terminal o PowerShell.", macIntro: "Abre Terminal, escribe cd y un espacio, arrastra esta carpeta de respaldo a la ventana y presiona Retorno.", runLabel: "Ejecuta:", footer: "Los scripts son texto sin formato y no están marcados como ejecutables. Cada comando requiere una acción explícita y única. En macOS las fechas se restauran con precisión de segundos.")
        case "fr":
            GuideCopy(languageTag: "fr", title: "Restaurer les dates des fichiers — Watermelon Backup", heading: "Restaurer les dates d’origine", intro: "Les navigateurs ne peuvent pas définir la date de modification lors de l’enregistrement. Watermelon Backup fournit donc deux scripts lisibles. Rien ne s’exécute automatiquement.", noteTitle: "Ce que fait le script :", noteBody: "Il lit les manifestes existants dans .watermelon/months/*.sqlite et modifie uniquement la date des fichiers de sauvegarde correspondants. Il ne se connecte pas à Internet et ne téléverse, supprime, renomme ou déplace aucun fichier.", wait: "Attendez la fin de la sauvegarde et la déconnexion de l’iPhone. Vous pouvez examiner le script dans un éditeur de texte avant de l’exécuter.", windowsIntro: "Ouvrez ce dossier de sauvegarde dans Windows Terminal ou PowerShell.", macIntro: "Ouvrez Terminal, saisissez cd suivi d’une espace, faites glisser ce dossier dans la fenêtre, puis appuyez sur Retour.", runLabel: "Exécutez :", footer: "Les scripts sont en texte brut et ne sont pas marqués comme exécutables. Chaque commande exige une action explicite et unique. Sous macOS, les dates sont restaurées à la seconde près.")
        case "ja":
            GuideCopy(languageTag: "ja", title: "ファイル日時を復元 — Watermelon Backup", heading: "元のファイル日時を復元", intro: "ブラウザは保存時に更新日時を設定できません。そのため Watermelon Backup は内容を確認できる2つのスクリプトを用意します。自動では実行されません。", noteTitle: "スクリプトの動作：", noteBody: ".watermelon/months/*.sqlite にある既存のマニフェストを読み、一致するバックアップファイルの更新日時だけを変更します。インターネット接続、アップロード、削除、名前変更、移動は行いません。", wait: "バックアップが完了し、iPhone が切断されてから実行してください。事前に任意のテキストエディタで内容を確認できます。", windowsIntro: "このバックアップフォルダを Windows Terminal または PowerShell で開きます。", macIntro: "ターミナルを開き、cd と空白を入力して、このバックアップフォルダをウインドウへドラッグし、Return キーを押します。", runLabel: "次を実行します：", footer: "スクリプトはプレーンテキストで、実行可能には設定されていません。各コマンドは明示的な1回の操作でのみ実行されます。macOS では秒単位の精度で更新日時を復元します。")
        case "ko":
            GuideCopy(languageTag: "ko", title: "파일 날짜 복원 — Watermelon Backup", heading: "원본 파일 날짜 복원", intro: "브라우저는 저장할 때 수정 날짜를 설정할 수 없습니다. 따라서 Watermelon Backup은 내용을 확인할 수 있는 스크립트 두 개를 제공합니다. 자동으로 실행되는 항목은 없습니다.", noteTitle: "스크립트의 작업:", noteBody: ".watermelon/months/*.sqlite의 기존 매니페스트를 읽고 일치하는 백업 파일의 수정 날짜만 변경합니다. 인터넷 연결, 업로드, 삭제, 이름 변경 또는 이동은 하지 않습니다.", wait: "백업이 끝나고 iPhone 연결이 해제된 후 실행하세요. 실행 전에 텍스트 편집기에서 스크립트를 확인할 수 있습니다.", windowsIntro: "이 백업 폴더를 Windows Terminal 또는 PowerShell에서 엽니다.", macIntro: "터미널을 열고 cd와 공백을 입력한 다음 이 백업 폴더를 창으로 드래그하고 Return을 누릅니다.", runLabel: "다음을 실행합니다:", footer: "스크립트는 일반 텍스트이며 실행 파일로 표시되지 않습니다. 각 명령은 사용자의 명시적인 일회성 작업으로만 실행됩니다. macOS에서는 초 단위 정밀도로 수정 날짜를 복원합니다.")
        case "pt-BR":
            GuideCopy(languageTag: "pt-BR", title: "Restaurar datas dos arquivos — Watermelon Backup", heading: "Restaurar as datas originais", intro: "Os navegadores não conseguem definir a data de modificação ao salvar. Por isso, o Watermelon Backup inclui dois scripts legíveis. Nada é executado automaticamente.", noteTitle: "O que o script faz:", noteBody: "Ele lê os manifestos existentes em .watermelon/months/*.sqlite e altera somente a data de modificação dos arquivos de backup correspondentes. Não acessa a internet nem envia, exclui, renomeia ou move arquivos.", wait: "Aguarde o fim do backup e a desconexão do iPhone. Você pode revisar o script em qualquer editor de texto antes de executá-lo.", windowsIntro: "Abra esta pasta de backup no Windows Terminal ou PowerShell.", macIntro: "Abra o Terminal, digite cd e um espaço, arraste esta pasta de backup para a janela e pressione Return.", runLabel: "Execute:", footer: "Os scripts são texto simples e não são marcados como executáveis. Cada comando exige uma ação explícita e única. No macOS, as datas são restauradas com precisão de segundos.")
        case "pt-PT":
            GuideCopy(languageTag: "pt-PT", title: "Restaurar datas dos ficheiros — Watermelon Backup", heading: "Restaurar as datas originais", intro: "Os navegadores não conseguem definir a data de modificação ao guardar. Por isso, o Watermelon Backup inclui dois scripts legíveis. Nada é executado automaticamente.", noteTitle: "O que o script faz:", noteBody: "Lê os manifestos existentes em .watermelon/months/*.sqlite e altera apenas a data de modificação dos ficheiros de cópia correspondentes. Não acede à Internet nem envia, elimina, muda o nome ou move ficheiros.", wait: "Aguarde o fim da cópia e a desconexão do iPhone. Pode rever o script num editor de texto antes de o executar.", windowsIntro: "Abra esta pasta de cópia no Windows Terminal ou PowerShell.", macIntro: "Abra o Terminal, escreva cd e um espaço, arraste esta pasta para a janela e prima Return.", runLabel: "Execute:", footer: "Os scripts são texto simples e não estão marcados como executáveis. Cada comando exige uma ação explícita e única. No macOS, as datas são restauradas com precisão de segundos.")
        case "ru":
            GuideCopy(languageTag: "ru", title: "Восстановление дат файлов — Watermelon Backup", heading: "Восстановление исходных дат файлов", intro: "Браузеры не могут задавать дату изменения при сохранении. Поэтому Watermelon Backup добавляет два читаемых сценария. Ничего не запускается автоматически.", noteTitle: "Что делает сценарий:", noteBody: "Он читает существующие манифесты .watermelon/months/*.sqlite и изменяет только даты подходящих файлов резервной копии. Он не подключается к Интернету, не загружает, не удаляет, не переименовывает и не перемещает файлы.", wait: "Дождитесь завершения резервного копирования и отключения iPhone. Перед запуском сценарий можно проверить в любом текстовом редакторе.", windowsIntro: "Откройте папку резервной копии в Windows Terminal или PowerShell.", macIntro: "Откройте Terminal, введите cd и пробел, перетащите папку резервной копии в окно и нажмите Return.", runLabel: "Выполните:", footer: "Сценарии являются обычным текстом и не помечены как исполняемые. Каждая команда запускается только явным однократным действием. В macOS даты восстанавливаются с точностью до секунды.")
        case "uk":
            GuideCopy(languageTag: "uk", title: "Відновлення дат файлів — Watermelon Backup", heading: "Відновлення початкових дат файлів", intro: "Браузери не можуть задавати дату зміни під час збереження. Тому Watermelon Backup додає два читабельні сценарії. Нічого не запускається автоматично.", noteTitle: "Що робить сценарій:", noteBody: "Він читає наявні маніфести .watermelon/months/*.sqlite і змінює лише дати відповідних файлів резервної копії. Він не підключається до Інтернету, не завантажує, не видаляє, не перейменовує та не переміщує файли.", wait: "Дочекайтеся завершення резервного копіювання та від’єднання iPhone. Перед запуском сценарій можна перевірити в будь-якому текстовому редакторі.", windowsIntro: "Відкрийте папку резервної копії у Windows Terminal або PowerShell.", macIntro: "Відкрийте Terminal, введіть cd і пробіл, перетягніть папку резервної копії у вікно та натисніть Return.", runLabel: "Виконайте:", footer: "Сценарії є звичайним текстом і не позначені як виконувані. Кожна команда запускається лише явною одноразовою дією. У macOS дати відновлюються з точністю до секунди.")
        case "zh-Hans":
            GuideCopy(languageTag: "zh-Hans", title: "恢复文件日期 — 西瓜备份", heading: "恢复文件的原始日期", intro: "浏览器在保存文件时无法设置修改日期，因此西瓜备份附带了两个可以直接阅读的脚本。任何内容都不会自动运行。", noteTitle: "脚本会做什么：", noteBody: "脚本读取 .watermelon/months/*.sqlite 中已有的清单，只修改匹配备份文件的修改日期。它不会连接互联网，也不会上传、删除、重命名或移动任何文件。", wait: "请等待备份完成并断开 iPhone。运行前，你可以先用任意文本编辑器检查脚本内容。", windowsIntro: "在 Windows Terminal 或 PowerShell 中打开这个备份文件夹。", macIntro: "打开“终端”，输入 cd 和一个空格，把这个备份文件夹拖入窗口，然后按下 Return。", runLabel: "运行：", footer: "这些脚本都是纯文本，也没有被标记为可执行文件。只有在你明确运行一次对应命令时才会执行。macOS 使用系统工具以整秒精度恢复修改日期。")
        case "zh-Hant":
            GuideCopy(languageTag: "zh-Hant", title: "還原檔案日期 — 西瓜備份", heading: "還原檔案的原始日期", intro: "瀏覽器在儲存檔案時無法設定修改日期，因此西瓜備份附帶兩個可以直接閱讀的指令碼。任何內容都不會自動執行。", noteTitle: "指令碼會做什麼：", noteBody: "指令碼讀取 .watermelon/months/*.sqlite 中現有的清單，只修改相符備份檔案的修改日期。它不會連接網際網路，也不會上傳、刪除、重新命名或移動任何檔案。", wait: "請等待備份完成並中斷 iPhone 連線。執行前，你可以先用任何文字編輯器檢查指令碼內容。", windowsIntro: "在 Windows Terminal 或 PowerShell 中開啟這個備份資料夾。", macIntro: "開啟「終端機」，輸入 cd 和一個空格，把這個備份資料夾拖入視窗，然後按下 Return。", runLabel: "執行：", footer: "這些指令碼都是純文字，也沒有標示為可執行檔。只有在你明確執行一次對應命令時才會執行。macOS 使用系統工具以整秒精度還原修改日期。")
        default:
            GuideCopy(languageTag: "en", title: "Restore file dates — Watermelon Backup", heading: "Restore original file dates", intro: "Browsers cannot set a file's modification date while saving it. Watermelon Backup therefore includes two readable scripts. Nothing runs automatically.", noteTitle: "What the script does:", noteBody: "It reads the existing manifests in .watermelon/months/*.sqlite and updates only the modification dates of matching backup files. It does not connect to the internet, upload, delete, rename, or move anything.", wait: "Wait until the backup has finished and the iPhone has disconnected. You can inspect the script in any text editor before running it.", windowsIntro: "Open this backup folder in Windows Terminal or PowerShell.", macIntro: "Open Terminal, type cd followed by a space, drag this backup folder into the window, then press Return.", runLabel: "Run:", footer: "The scripts are plain text and are not marked as executable. Each command runs only through your explicit, one-time action. On macOS, the system tools restore modification dates with whole-second precision.")
        }
    }

    private static func resolvedLanguage(_ identifier: String) -> String {
        let value = identifier.replacingOccurrences(of: "_", with: "-").lowercased()
        if value == "es-419" || value.hasPrefix("es-419-") { return "es-419" }
        if value.hasPrefix("pt-br") { return "pt-BR" }
        if value.hasPrefix("pt-pt") { return "pt-PT" }
        if value == "zh-hk" || value.hasPrefix("zh-hant") || value.hasPrefix("zh-tw") || value.hasPrefix("zh-mo") { return "zh-Hant" }
        if value.hasPrefix("zh-hans") || value.hasPrefix("zh-cn") || value.hasPrefix("zh-sg") { return "zh-Hans" }
        let base = value.split(separator: "-").first.map(String.init) ?? "en"
        return ["de", "es", "fr", "ja", "ko", "ru", "uk"].contains(base) ? base : "en"
    }

    private static func htmlEscaped(_ value: String) -> String {
        value
            .replacingOccurrences(of: "&", with: "&amp;")
            .replacingOccurrences(of: "<", with: "&lt;")
            .replacingOccurrences(of: ">", with: "&gt;")
            .replacingOccurrences(of: "\"", with: "&quot;")
            .replacingOccurrences(of: "'", with: "&#39;")
    }

    private struct TerminalCopy {
        let scanning: String
        let progress: String
        let completed: String
        let updated: String
        let skipped: String
        let errors: String
    }

    private static func progressNote(languageIdentifier: String) -> String {
        switch resolvedLanguage(languageIdentifier) {
        case "de": "Das Skript zeigt während der Verarbeitung den Fortschritt an. Schließen Sie das Terminal erst nach der Abschlussübersicht."
        case "es": "El script muestra el progreso mientras trabaja. No cierres la terminal hasta que aparezca el resumen final."
        case "es-419": "El script muestra el progreso mientras trabaja. No cierres la terminal hasta que aparezca el resumen final."
        case "fr": "Le script affiche sa progression. Ne fermez le terminal qu’après l’affichage du récapitulatif final."
        case "ja": "処理中は進行状況が表示されます。完了結果が表示されるまでターミナルを閉じないでください。"
        case "ko": "처리 중에는 진행률이 표시됩니다. 완료 요약이 나타날 때까지 터미널을 닫지 마세요."
        case "pt-BR": "O script mostra o progresso durante o processamento. Feche o terminal somente depois que o resumo final aparecer."
        case "pt-PT": "O script mostra o progresso durante o processamento. Feche o terminal apenas depois de aparecer o resumo final."
        case "ru": "Во время работы сценарий показывает ход выполнения. Не закрывайте терминал до появления итоговой сводки."
        case "uk": "Під час роботи сценарій показує перебіг виконання. Не закривайте термінал до появи підсумку."
        case "zh-Hans": "脚本运行时会显示进度。看到完成汇总后再关闭终端窗口。"
        case "zh-Hant": "指令碼執行時會顯示進度。看到完成摘要後再關閉終端機視窗。"
        default: "The script shows progress while it works. Close the terminal only after the completion summary appears."
        }
    }

    private static func terminalCopy(languageIdentifier: String) -> TerminalCopy {
        switch resolvedLanguage(languageIdentifier) {
        case "de": TerminalCopy(scanning: "Sicherungsdaten werden geprüft", progress: "Dateidaten werden wiederhergestellt", completed: "Abgeschlossen", updated: "Aktualisiert", skipped: "Übersprungen", errors: "Fehler")
        case "es", "es-419": TerminalCopy(scanning: "Comprobando los datos de copia", progress: "Restaurando fechas", completed: "Completado", updated: "Actualizados", skipped: "Omitidos", errors: "Errores")
        case "fr": TerminalCopy(scanning: "Vérification des données de sauvegarde", progress: "Restauration des dates", completed: "Terminé", updated: "Modifiés", skipped: "Ignorés", errors: "Erreurs")
        case "ja": TerminalCopy(scanning: "バックアップデータを確認中", progress: "ファイル日時を復元中", completed: "完了", updated: "更新", skipped: "スキップ", errors: "エラー")
        case "ko": TerminalCopy(scanning: "백업 데이터 확인 중", progress: "파일 날짜 복원 중", completed: "완료", updated: "업데이트", skipped: "건너뜀", errors: "오류")
        case "pt-BR", "pt-PT": TerminalCopy(scanning: "Verificando os dados do backup", progress: "Restaurando datas", completed: "Concluído", updated: "Atualizados", skipped: "Ignorados", errors: "Erros")
        case "ru": TerminalCopy(scanning: "Проверка данных резервной копии", progress: "Восстановление дат", completed: "Готово", updated: "Обновлено", skipped: "Пропущено", errors: "Ошибки")
        case "uk": TerminalCopy(scanning: "Перевірка даних резервної копії", progress: "Відновлення дат", completed: "Готово", updated: "Оновлено", skipped: "Пропущено", errors: "Помилки")
        case "zh-Hans": TerminalCopy(scanning: "正在检查备份数据", progress: "正在恢复文件修改日期", completed: "已完成", updated: "已更新", skipped: "已跳过", errors: "错误")
        case "zh-Hant": TerminalCopy(scanning: "正在檢查備份資料", progress: "正在還原檔案修改日期", completed: "已完成", updated: "已更新", skipped: "已略過", errors: "錯誤")
        default: TerminalCopy(scanning: "Checking backup data", progress: "Restoring file dates", completed: "Completed", updated: "Updated", skipped: "Skipped", errors: "Errors")
        }
    }

    private static func powerShellEscaped(_ value: String) -> String {
        value
            .replacingOccurrences(of: "`", with: "``")
            .replacingOccurrences(of: "$", with: "`$")
            .replacingOccurrences(of: "\"", with: "`\"")
    }

    private static func shellSingleQuoted(_ value: String) -> String {
        "'" + value.replacingOccurrences(of: "'", with: "'\\''") + "'"
    }

    private static func windowsScript(languageIdentifier: String) -> String {
        let copy = terminalCopy(languageIdentifier: languageIdentifier)
        return #"""
    param([Parameter(Mandatory=$true)][string]$Root)
    $ErrorActionPreference = "Stop"

    if (-not ("WatermelonSQLite" -as [type])) {
    Add-Type -TypeDefinition @'
    using System;
    using System.Collections.Generic;
    using System.Runtime.InteropServices;
    using System.Text;

    public sealed class WatermelonManifestRow
    {
        public long Milliseconds { get; private set; }
        public string FileName { get; private set; }

        public WatermelonManifestRow(long milliseconds, string fileName)
        {
            Milliseconds = milliseconds;
            FileName = fileName;
        }
    }

    public static class WatermelonSQLite
    {
        private const int SQLITE_OPEN_READONLY = 1;
        private const int SQLITE_ROW = 100;
        private const int SQLITE_DONE = 101;
        private const int SQLITE_NULL = 5;
        private const uint LOAD_LIBRARY_SEARCH_SYSTEM32 = 0x00000800;

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern IntPtr LoadLibraryEx(string fileName, IntPtr file, uint flags);

        [DllImport("winsqlite3.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern int sqlite3_open_v2(byte[] filename, out IntPtr database, int flags, IntPtr vfs);

        [DllImport("winsqlite3.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern int sqlite3_prepare_v2(
            IntPtr database,
            byte[] sql,
            int byteCount,
            out IntPtr statement,
            IntPtr tail
        );

        [DllImport("winsqlite3.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern int sqlite3_step(IntPtr statement);

        [DllImport("winsqlite3.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern int sqlite3_column_type(IntPtr statement, int column);

        [DllImport("winsqlite3.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern long sqlite3_column_int64(IntPtr statement, int column);

        [DllImport("winsqlite3.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr sqlite3_column_text(IntPtr statement, int column);

        [DllImport("winsqlite3.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern int sqlite3_column_bytes(IntPtr statement, int column);

        [DllImport("winsqlite3.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern int sqlite3_finalize(IntPtr statement);

        [DllImport("winsqlite3.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern int sqlite3_close(IntPtr database);

        static WatermelonSQLite()
        {
            if (LoadLibraryEx("winsqlite3.dll", IntPtr.Zero, LOAD_LIBRARY_SEARCH_SYSTEM32) == IntPtr.Zero)
                throw new InvalidOperationException("The Windows system SQLite library is unavailable.");
        }

        public static WatermelonManifestRow[] Read(string path)
        {
            IntPtr database = IntPtr.Zero;
            IntPtr statement = IntPtr.Zero;
            int openCode = sqlite3_open_v2(Utf8Z(path), out database, SQLITE_OPEN_READONLY, IntPtr.Zero);
            if (openCode != 0)
            {
                if (database != IntPtr.Zero) sqlite3_close(database);
                throw new InvalidOperationException("Cannot open manifest (" + openCode + ").");
            }

            try
            {
                byte[] query = Utf8Z(
                    "SELECT creationDateMs, fileName FROM resources " +
                    "WHERE creationDateMs IS NOT NULL ORDER BY fileName"
                );
                int prepareCode = sqlite3_prepare_v2(database, query, query.Length - 1, out statement, IntPtr.Zero);
                if (prepareCode != 0) throw new InvalidOperationException("Cannot read manifest (" + prepareCode + ").");

                var rows = new List<WatermelonManifestRow>();
                while (true)
                {
                    int stepCode = sqlite3_step(statement);
                    if (stepCode == SQLITE_DONE) break;
                    if (stepCode != SQLITE_ROW) throw new InvalidOperationException("Manifest query failed (" + stepCode + ").");
                    if (sqlite3_column_type(statement, 0) == SQLITE_NULL) continue;

                    IntPtr text = sqlite3_column_text(statement, 1);
                    int length = sqlite3_column_bytes(statement, 1);
                    if (text == IntPtr.Zero || length <= 0 || length > 32768) continue;
                    byte[] bytes = new byte[length];
                    Marshal.Copy(text, bytes, 0, length);
                    rows.Add(new WatermelonManifestRow(
                        sqlite3_column_int64(statement, 0),
                        Encoding.UTF8.GetString(bytes)
                    ));
                }
                return rows.ToArray();
            }
            finally
            {
                if (statement != IntPtr.Zero) sqlite3_finalize(statement);
                if (database != IntPtr.Zero) sqlite3_close(database);
            }
        }

        private static byte[] Utf8Z(string value)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(value);
            byte[] terminated = new byte[bytes.Length + 1];
            Buffer.BlockCopy(bytes, 0, terminated, 0, bytes.Length);
            return terminated;
        }
    }
    '@
    }

    $rootPath = [IO.Path]::GetFullPath($Root)
    $separator = [string][IO.Path]::DirectorySeparatorChar
    if (-not $rootPath.EndsWith($separator)) { $rootPath += $separator }
    $manifestDirectory = Join-Path $rootPath ".watermelon\months"
    $updated = 0
    $skipped = 0
    $failed = 0
    $processed = 0
    $lastPercent = -1
    $manifests = @(
        Get-ChildItem -LiteralPath $manifestDirectory -Filter "*.sqlite" -File -ErrorAction SilentlyContinue |
            Where-Object { $_.BaseName -match '^\d{4}-(0[1-9]|1[0-2])$' } |
            Sort-Object Name
    )
    $manifestCount = $manifests.Count
    $manifestIndex = 0

    Write-Host "\#(powerShellEscaped(copy.scanning))..."
    foreach ($manifest in $manifests) {
            $year = $manifest.BaseName.Substring(0, 4)
            $month = $manifest.BaseName.Substring(5, 2)
            try {
                $rows = [WatermelonSQLite]::Read($manifest.FullName)
            } catch {
                $failed += 1
                $manifestIndex += 1
                continue
            }

            $rowIndex = 0
            $rowCount = $rows.Count
            foreach ($row in $rows) {
                $rowIndex += 1
                $processed += 1
                try {
                    $name = [string]$row.FileName
                    if ([String]::IsNullOrEmpty($name) -or $name -eq "." -or $name -eq "..") { $skipped += 1; continue }
                    if ([IO.Path]::IsPathRooted($name) -or $name.Contains('\') -or $name.Contains('/')) { $skipped += 1; continue }
                    $target = [IO.Path]::GetFullPath((Join-Path (Join-Path (Join-Path $rootPath $year) $month) $name))
                    if (-not $target.StartsWith($rootPath, [StringComparison]::OrdinalIgnoreCase)) { $skipped += 1; continue }

                    $cursor = $rootPath.TrimEnd([char][IO.Path]::DirectorySeparatorChar)
                    $safe = $true
                    foreach ($segment in @($year, $month, $name)) {
                        $cursor = Join-Path $cursor $segment
                        if (Test-Path -LiteralPath $cursor) {
                            $item = Get-Item -LiteralPath $cursor -Force
                            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                                $safe = $false
                                break
                            }
                        }
                    }

                    if ($safe -and [IO.File]::Exists($target)) {
                        [IO.File]::SetLastWriteTimeUtc(
                            $target,
                            [DateTimeOffset]::FromUnixTimeMilliseconds([Int64]$row.Milliseconds).UtcDateTime
                        )
                        $updated += 1
                    } else {
                        $skipped += 1
                    }
                } catch {
                    $failed += 1
                } finally {
                    $withinManifest = if ($rowCount -gt 0) { $rowIndex / $rowCount } else { 1 }
                    $percent = if ($manifestCount -gt 0) {
                        [Math]::Min(100, [Math]::Floor((($manifestIndex + $withinManifest) / $manifestCount) * 100))
                    } else { 100 }
                    if ($percent -ne $lastPercent) {
                        Write-Progress -Activity "\#(powerShellEscaped(copy.progress))" -Status "$($manifest.Name) — $processed" -PercentComplete $percent
                        $lastPercent = $percent
                    }
                }
            }
            $manifestIndex += 1
    }
    Write-Progress -Activity "\#(powerShellEscaped(copy.progress))" -Completed
    Write-Host "\#(powerShellEscaped(copy.completed)). \#(powerShellEscaped(copy.updated)): $updated, \#(powerShellEscaped(copy.skipped)): $skipped, \#(powerShellEscaped(copy.errors)): $failed"
    """#
    }

    private static func macScript(languageIdentifier: String) -> String {
        let copy = terminalCopy(languageIdentifier: languageIdentifier)
        return #"""
    #!/bin/zsh
    set -u
    setopt NULL_GLOB PIPE_FAIL
    root="${1:A}"
    updated=0
    skipped=0
    failed=0
    processed=0
    lastPercent=-1
    manifests=()

    for manifest in "$root"/.watermelon/months/[0-9][0-9][0-9][0-9]-[0-9][0-9].sqlite; do
        stem="${manifest:t:r}"
        [[ "$stem" =~ '^[0-9]{4}-(0[1-9]|1[0-2])$' ]] || continue
        manifests+=("$manifest")
    done

    manifestCount=${#manifests}
    manifestIndex=0
    print -r -- \#(shellSingleQuoted(copy.scanning + "..."))

    for manifest in "${manifests[@]}"; do
        stem="${manifest:t:r}"
        year="${stem[1,4]}"
        month="${stem[6,7]}"
        rowCount="$(/usr/bin/sqlite3 -readonly -noheader "$manifest" 'SELECT count(*) FROM resources WHERE creationDateMs IS NOT NULL' 2>/dev/null)" || {
            (( failed += 1, manifestIndex += 1 ))
            continue
        }
        [[ "$rowCount" == <-> ]] || {
            (( failed += 1, manifestIndex += 1 ))
            continue
        }
        rowIndex=0

        while IFS='|' read -r milliseconds hexName; do
            (( rowIndex += 1, processed += 1 ))
            percent=$(( manifestCount > 0 ? ((manifestIndex * 100) + (rowCount > 0 ? rowIndex * 100 / rowCount : 100)) / manifestCount : 100 ))
            if (( percent != lastPercent )); then
                printf '\r%s %3d%% — %s — %d' \#(shellSingleQuoted(copy.progress)) "$percent" "$stem" "$processed"
                lastPercent=$percent
            fi

            [[ ("$milliseconds" == <-> || "$milliseconds" == -<->) && -n "$hexName" && "$hexName" != *[^0-9A-F]* ]] || { (( skipped += 1 )); continue; }
            (( ${#hexName} % 2 == 0 )) || { (( skipped += 1 )); continue; }
            decodedName="$({ printf '%s' "$hexName" | /usr/bin/xxd -r -p && printf '\x1f'; } 2>/dev/null)" || { (( failed += 1 )); continue; }
            [[ "$decodedName" == *$'\x1f' ]] || { (( failed += 1 )); continue; }
            fileName="${decodedName%$'\x1f'}"
            [[ -n "$fileName" && "$fileName" != "." && "$fileName" != ".." ]] || { (( skipped += 1 )); continue; }
            [[ "$fileName" != */* && "$fileName" != *$'\n'* && "$fileName" != *$'\r'* ]] || { (( skipped += 1 )); continue; }

            target="$root/$year/$month/$fileName"
            [[ -f "$target" && ! -L "$target" ]] || { (( skipped += 1 )); continue; }
            resolved="${target:A}"
            [[ "$resolved" == "$root"/* ]] || { (( skipped += 1 )); continue; }
            if (( milliseconds < 0 )); then
                seconds=$(( (milliseconds - 999) / 1000 ))
            else
                seconds=$(( milliseconds / 1000 ))
            fi
            /usr/bin/perl -e 'my @s = stat($ARGV[1]); die $! unless @s; utime($s[8], $ARGV[0], $ARGV[1]) or die $!' -- "$seconds" "$resolved" || { (( failed += 1 )); continue; }
            (( updated += 1 ))
        done < <(
            /usr/bin/sqlite3 -readonly -noheader -separator '|' "$manifest" \
                'SELECT creationDateMs, hex(CAST(fileName AS BLOB)) FROM resources WHERE creationDateMs IS NOT NULL ORDER BY fileName' \
                2>/dev/null
        )
        (( manifestIndex += 1 ))
    done

    printf '\n'
    printf '%s. %s: %d, %s: %d, %s: %d\n' \#(shellSingleQuoted(copy.completed)) \#(shellSingleQuoted(copy.updated)) "$updated" \#(shellSingleQuoted(copy.skipped)) "$skipped" \#(shellSingleQuoted(copy.errors)) "$failed"
    """#
    }
}

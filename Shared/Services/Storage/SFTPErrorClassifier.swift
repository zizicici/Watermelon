import Citadel
import Foundation
import NIOCore
import NIOPosix

enum SFTPErrorClassifier {
    static func describe(_ error: Error) -> String {
        if let mismatch = error as? SFTPHostKeyMismatchError {
            return mismatch.errorDescription ?? error.localizedDescription
        }

        if let storage = error as? RemoteStorageClientError {
            switch storage {
            case .underlying(let inner):
                return describe(inner)
            case .invalidConfiguration:
                return String(localized: "sftp.error.invalidConfiguration")
            default:
                return storage.errorDescription ?? error.localizedDescription
            }
        }

        if error is AuthenticationFailed {
            return String(localized: "sftp.error.authenticationFailed")
        }

        if let sshError = error as? SSHClientError {
            switch sshError {
            case .allAuthenticationOptionsFailed:
                return String(localized: "sftp.error.authenticationFailed")
            case .unsupportedPasswordAuthentication:
                return String(localized: "sftp.error.serverRejectsPassword")
            case .unsupportedPrivateKeyAuthentication:
                return String(localized: "sftp.error.serverRejectsPrivateKey")
            case .unsupportedHostBasedAuthentication, .channelCreationFailed:
                return error.localizedDescription
            }
        }

        if let sftpError = error as? SFTPError {
            switch sftpError {
            case .errorStatus(let status):
                return describeStatus(status)
            case .connectionClosed:
                return String(localized: "sftp.error.connectionClosed")
            default:
                return error.localizedDescription
            }
        }

        if isConnectionUnavailable(error) {
            return String(localized: "sftp.error.cannotConnect")
        }

        return error.localizedDescription
    }

    static func isConnectionUnavailable(_ error: Error) -> Bool {
        if error is SFTPHostKeyMismatchError { return false }
        if let storage = error as? RemoteStorageClientError {
            switch storage {
            case .notConnected, .unavailable:
                return true
            case .underlying(let inner):
                return isConnectionUnavailable(inner)
            default:
                return false
            }
        }
        if let sftp = error as? SFTPError, case .connectionClosed = sftp { return true }
        if error is NIOConnectionError { return true }
        if let channel = error as? ChannelError, case .connectTimeout = channel { return true }
        let nsError = error as NSError
        if nsError.domain == NSPOSIXErrorDomain {
            let networkCodes: Set<Int> = [
                Int(ECONNREFUSED),
                Int(EHOSTUNREACH),
                Int(ENETUNREACH),
                Int(ETIMEDOUT),
                Int(ECONNRESET)
            ]
            return networkCodes.contains(nsError.code)
        }
        return false
    }

    private static func describeStatus(_ status: SFTPMessage.Status) -> String {
        switch status.errorCode {
        case .noSuchFile:
            return String(localized: "sftp.error.noSuchFile")
        case .permissionDenied:
            return String(localized: "sftp.error.permissionDenied")
        default:
            let serverMessage = status.message.trimmingCharacters(in: .whitespacesAndNewlines)
            let detail = serverMessage.isEmpty ? status.errorCode.debugDescription : serverMessage
            return String.localizedStringWithFormat(
                String(localized: "sftp.error.serverStatus"),
                detail
            )
        }
    }
}

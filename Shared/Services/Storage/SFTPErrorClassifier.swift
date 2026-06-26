import Citadel
import Foundation
import NIOCore
import NIOPosix
import NIOSSH

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

        if error is NIOSSHError {
            return String(localized: "sftp.error.cannotConnect")
        }

        if isConnectionUnavailable(error) {
            return String(localized: "sftp.error.cannotConnect")
        }

        return error.localizedDescription
    }

    nonisolated static func isNotFound(_ error: Error) -> Bool {
        if error is CancellationError { return false }
        if isConnectionUnavailable(error) { return false }
        if let storage = error as? RemoteStorageClientError {
            switch storage {
            case .underlying(let inner):
                return isNotFound(inner)
            default:
                return false
            }
        }
        if let sftp = error as? SFTPError, case .errorStatus(let status) = sftp {
            return isNotFoundStatus(status)
        }
        if let status = error as? SFTPMessage.Status {
            return isNotFoundStatus(status)
        }
        let nsError = error as NSError
        if nsError.domain == NSPOSIXErrorDomain, nsError.code == Int(ENOENT) {
            return true
        }
        if let underlying = nsError.userInfo[NSUnderlyingErrorKey] as? Error {
            return isNotFound(underlying)
        }
        return false
    }

    nonisolated static func isConnectionUnavailable(_ error: Error) -> Bool {
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
        if let sftp = error as? SFTPError {
            if case .connectionClosed = sftp { return true }
            // The server reported the SFTP connection died mid-request — a reconnect recovers it.
            if case .errorStatus(let status) = sftp,
               isConnectionUnavailableStatus(status) { return true }
        }
        if let status = error as? SFTPMessage.Status {
            return isConnectionUnavailableStatus(status)
        }
        if error is NIOSSHError { return true }
        if error is NIOConnectionError { return true }
        if let channel = error as? ChannelError, case .connectTimeout = channel { return true }
        let nsError = error as NSError
        if let underlying = nsError.userInfo[NSUnderlyingErrorKey] as? Error,
           isConnectionUnavailable(underlying) {
            return true
        }
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

    private nonisolated static func isNotFoundStatus(_ status: SFTPMessage.Status) -> Bool {
        status.errorCode == .noSuchFile
    }

    private nonisolated static func isConnectionUnavailableStatus(_ status: SFTPMessage.Status) -> Bool {
        status.errorCode == .connectionLost || status.errorCode == .noConnection
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

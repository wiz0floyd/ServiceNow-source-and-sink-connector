# Encode a PKCS12 keystore as base64 for the connector config.
# Usage:
#   .\scripts\encode-keystore.ps1 -KeystorePath C:\path\to\keystore.p12
#   .\scripts\encode-keystore.ps1 -KeystorePath C:\path\to\keystore.p12 -Clipboard

param(
    [Parameter(Mandatory=$true)]
    [string]$KeystorePath,

    [switch]$Clipboard
)

if (-not (Test-Path $KeystorePath)) {
    Write-Error "File not found: $KeystorePath"
    exit 1
}

$bytes = [IO.File]::ReadAllBytes($KeystorePath)
$b64 = [Convert]::ToBase64String($bytes)

Write-Output "File:    $KeystorePath"
Write-Output "Bytes:   $($bytes.Length)"
Write-Output "Base64:  $($b64.Length) chars"
Write-Output ""

if ($Clipboard) {
    $b64 | Set-Clipboard
    Write-Output "(copied to clipboard)"
} else {
    Write-Output "--- BEGIN BASE64 ---"
    Write-Output $b64
    Write-Output "--- END BASE64 ---"
}

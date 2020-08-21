param([Parameter()] [ValidateNotNullOrEmpty()] [string]$thumbprint=$(throw "thumbprint is mandatory, please provide a value."))
Install-Module -Name SqlServer -AllowClobber -Force -Scope CurrentUser
$TargetCmkSettings = New-SqlCertificateStoreColumnMasterKeySettings -CertificateStoreLocation "CurrentUser" -Thumbprint "$thumbprint"
New-SqlColumnEncryptionKeyEncryptedValue -TargetColumnMasterKeySettings $TargetCmkSettings
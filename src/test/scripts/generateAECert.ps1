# Run this script from mssql-jdbc root folder
Remove-Item -Path AE_Certificates -Force -Recurse -ErrorAction SilentlyContinue
mkdir AE_Certificates | Out-Null
cd AE_Certificates
$cert = New-SelfSignedCertificate -DnsName "AlwaysEncryptedCert" -CertStoreLocation Cert:CurrentUser\My -KeyExportPolicy Exportable -Type DocumentEncryptionCert -KeyUsage KeyEncipherment -KeySpec KeyExchange -KeyLength 2048
$pwd = ConvertTo-SecureString -String "password" -Force -AsPlainText
$path = 'cert:\CurrentUser\My\' + $cert.thumbprint
$certificate = Export-PfxCertificate -cert $path -FilePath cert.pfx -Password $pwd
Get-ChildItem -path cert:\CurrentUser\My > certificate.txt
(keytool -importkeystore -srckeystore cert.pfx -srcstoretype pkcs12 -destkeystore clientcert.jks -deststoretype pkcs12 -srcstorepass password -deststorepass password) | out-null
keytool -list -v -keystore clientcert.jks -storepass "password" > JavaKeyStoreBase.txt
Get-Content .\JavaKeyStoreBase.txt | Set-Content -Encoding utf8 JavaKeyStore.txt
Remove-Item .\JavaKeyStoreBase.txt
cd ..
Write-Output $cert.thumbprint
exit $LASTEXITCODE
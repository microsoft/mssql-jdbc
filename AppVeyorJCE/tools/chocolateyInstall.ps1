$script_path = $(Split-Path -parent $MyInvocation.MyCommand.Definition)
$common = $(Join-Path $script_path "common.ps1")
. $common

#installs JCE
try {
    chocolatey-install
} catch {
    if ($_.Exception.InnerException) {
        $msg = $_.Exception.InnerException.Message
    } else {
        $msg = $_.Exception.Message
    }
    throw 
}  

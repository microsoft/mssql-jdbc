$jce_version = '7' 
$zipFolder = 'UnlimitedJCEPolicy'
$script_path = $(Split-Path -parent $MyInvocation.MyCommand.Definition)

function has_file($filename) {
    return Test-Path $filename
}

function download-from-oracle($url, $output_filename) {
    if (!(has_file($output_fileName))) {
        Write-Host  "Downloading JCE from $url"

        try {
            [System.Net.ServicePointManager]::ServerCertificateValidationCallback = { $true }
            $client = New-Object Net.WebClient
            $dummy = $client.Headers.Add('Cookie', 'gpw_e24=http://www.oracle.com; oraclelicense=accept-securebackup-cookie')
            $dummy = $client.DownloadFile($url, $output_filename)
        } finally {
            [System.Net.ServicePointManager]::ServerCertificateValidationCallback = $null
        }
    }  
}

function download-jce-file($url, $output_filename) {
    $dummy = download-from-oracle $url $output_filename
}

function download-jce() {
    $filename = "UnlimitedJCEPolicyJDK$jce_version.zip"
    $url = "http://download.oracle.com/otn-pub/java/jce/$jce_version/$filename"
    $output_filename = Join-Path $script_path $filename
	If(!(Test-Path $output_filename)){
		$dummy = download-jce-file $url $output_filename
	}
    return $output_filename
}

function get-java-home(){
    return Get-EnvironmentVariable 'JAVA_HOME' -Scope 'Machine' -PreserveVariables
}

function get-jce-dir($java_home) {
    return Join-Path $java_home 'jre\lib\security'
}

function chocolatey-install() {
    $java_home = get-java-home
    if (!$java_home) { 
        Write-Host "Couldnt find JAVA_HOME environment variable"
        Write-Host "Skipping installation"
    }else{
    	$jce_dir = get-jce-dir $java_home
	    $already_patched_file = Join-Path $jce_dir 'local_policy_old.jar'
	
	    If(Test-Path $already_patched_file){
		    Write-Host "JCE already installed: $jce_dir"
		    Write-Host "Skipping installation"
	    }else{
		    Write-Host "JCE is not installed ($already_patched_file) is not present"
		    Write-Host "Starting installation"
		    install-jce $jce_dir
	    }
    }
}

function install-jce($jce_dir) {
	$jce_zip_file = download-jce
	$temp_dir = Get-EnvironmentVariable 'TEMP' -Scope User -PreserveVariables
	$local_policy = Join-Path $jce_dir 'local_policy.jar'
	$export_policy = Join-Path $jce_dir 'US_export_policy.jar'

	Write-Host "Downloading JCE ($jce_zip_file)"
	Install-ChocolateyZipPackage -PackageName 'jce7' -Url $jce_zip_file -UnzipLocation $temp_dir
	
	If(Test-Path $local_policy){
		Rename-Item -Path $local_policy -NewName 'local_policy_old.jar' -Force
	}
	
	If(Test-Path $export_policy){
		Rename-Item -Path $export_policy -NewName 'US_export_policy_old.jar' -Force
	}
	
	$unzippedFolder = Join-Path $temp_dir $zipFolder
	Copy-Item $unzippedFolder\*.jar $jce_dir -force
}

# JCE chocolatey package

### Disclaimers:
1. All contents within this directory originate from [this GitHub project](https://github.com/TobseF/jce-chocolatey-package). This project was added to allow us to test the Always Encrypted feature on AppVeyor builds.

2. This is not an official project of Oracle. It\`s only easy of the manual installation: It downloads the JCE from oracle.com and unpacks it to the installed JDK.


[Chocolatey](https://chocolatey.org/) package for the [JCE (Unlimited Strength Java Cryptography Extension Policy Files)](http://www.oracle.com/technetwork/java/javase/downloads/jce-7-download-432124.html)

This chocolatey package adds the JCE to latest installed Java SDK. The The `JAVA_HOME` environment variable has to point to the JDK. If `JAVA_HOME` is not set, nothing will be changed. The original files are backuped (renamed to `*_old`) and can be reverted at any time. This package is a perfect addion to the [JDK8 package](https://chocolatey.org/packages/jdk8).

#### Install with [Chocolatey](https://chocolatey.org/)
```PowerShell
choco install jce -y
```

#### Build from source:
1. Install [Chocolatey](https://chocolatey.org/).
2. Open cmd with admin rights in jce package directory.
3. Pack NuGet Package (.nupkg).
```PowerShell
cpack
```
4. Install JCE NuGet Package.
```PowerShell
choco install jce -fdv -s . -y
```




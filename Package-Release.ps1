$Build = [Regex]::Match([IO.File]::ReadAllText("DataBoss\Properties\Version.cs"), "AssemblyInformationalVersion\(\""(?<build>.+)\""\)").Groups["build"].Value

function Args {
    param([parameter(ValueFromRemainingArguments=$true)] [String[]] $args)
    $args
}

$BuildArgs = Args /v:m /nologo /p:Configuration=Release /p:Build=$Build
$PackArgs = $BuildArgs + (Args /t:Pack /p:PackageVersion=$Build /p:DataBossPackageVersion=$Build /p:PackageOutputPath="..\Build")

function MSBuild {
    param([parameter(ValueFromRemainingArguments=$true)] [String[]] $args)
    & dotnet msbuild ($args + $BuildArgs)
}

function NuPack {
    param([parameter(ValueFromRemainingArguments=$true)] [String[]] $args)
    & dotnet msbuild ($args + $PackArgs) 
}

Remove-Item -Path Build -Recurse -Force
MSBuild Build.proj /t:Restore /t:Build /p:Configuration=Release /p:Build=$Build

Write-Host Packgaging version $Build
NuPack DataBoss.Data/DataBoss.Data.csproj /t:Restore
NuPack DataBoss\DataBoss.csproj /t:Restore

$Build = [Regex]::Match([IO.File]::ReadAllText("DataBoss\Properties\Version.cs"), "AssemblyInformationalVersion\(\""(?<build>.+)\""\)").Groups["build"].Value

& dotnet msbuild Build.proj "/t:Clean;Restore;Build" /p:Configuration=Release /p:Build=$Build /v:m /nologo

Write-Host Packgaging version $Build
& dotnet msbuild DataBoss.Data/DataBoss.Data.csproj /t:Pack /p:Configuration=Release /p:Build=$Build /p:PackageVersion=$Build /v:m /nologo /p:DataBossPackageVersion=$Build /p:PackageOutputPath="..\Build"
& dotnet msbuild DataBoss\DataBoss.csproj /t:Restore /t:Pack /p:Configuration=Release /p:Build=$Build /p:PackageVersion=$Build /p:DataBossPackageVersion=$Build /p:PackageOutputPath="..\Build"

$Build = [Regex]::Match([IO.File]::ReadAllText("DataBoss\Properties\Version.cs"), "AssemblyInformationalVersion\(\""(?<build>.+)\""\)").Groups["build"].Value

Remove-Item -Path Build -Recurse -Force
& dotnet msbuild Build.proj /t:Restore /t:Build /p:Configuration=Release /p:Build=$Build /v:m /nologo

Write-Host Packgaging version $Build
& dotnet msbuild DataBoss.Data/DataBoss.Data.csproj /t:Pack /p:Configuration=Release /p:Build=$Build /p:PackageVersion=$Build /v:m /nologo /p:DataBossPackageVersion=$Build /p:PackageOutputPath="..\Build"
& dotnet msbuild DataBoss\DataBoss.csproj /t:Restore /t:Pack /p:Configuration=Release /p:Build=$Build /p:PackageVersion=$Build /p:DataBossPackageVersion=$Build /p:PackageOutputPath="..\Build"

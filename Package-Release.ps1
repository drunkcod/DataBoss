$Build = [Regex]::Match([IO.File]::ReadAllText("DataBoss\Properties\Version.cs"), "AssemblyInformationalVersion\(\""(?<build>.+)\""\)").Groups["build"].Value

& dotnet msbuild DataBoss\DataBoss.sln /t:Clean /p:Configuration=Release /v:m /nologo
& dotnet msbuild Build.proj /t:Build /p:Configuration=Release /p:Build=$Build /v:m /nologo

& dotnet msbuild DataBoss\DataBoss.csproj /t:Pack /p:OutputDirectory=Artifacts /p:Configuration=Release /p:Build=$Build /p:PackageVersion=$Build
& dotnet msbuild DataBoss.Data/DataBoss.Data.csproj /t:Pack /p:Configuration=Release /p:Build=$Build /p:PackageVersion=$Build /v:m /nologo 

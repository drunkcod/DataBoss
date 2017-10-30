$Build = [Regex]::Match([IO.File]::ReadAllText("DataBoss\Properties\Version.cs"), "AssemblyVersion\(\""(?<build>.+)\""\)").Groups["build"].Value

& dotnet msbuild DataBoss\DataBoss.sln /t:Clean /p:Configuration=Release /v:m /nologo
& dotnet msbuild Build.proj /t:Build /p:Configuration=Release /p:Build=$Build /v:m /nologo

#& ./Tools/NuGet.exe pack DataBoss\DataBoss.csproj -OutputDirectory Artifacts -Properties "Configuration=Release;id=DataBoss"
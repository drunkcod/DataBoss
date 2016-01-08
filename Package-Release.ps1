$Build = [Regex]::Match([IO.File]::ReadAllText("DataBoss\Properties\Version.cs"), "AssemblyVersion\(\""(?<build>.+)\""\)").Groups["build"].Value

$MSBuildPath = [IO.Directory]::GetFiles(
	[IO.Path]::Combine([Environment]::GetFolderPath([Environment+SpecialFolder]::ProgramFilesX86), "MSBuild"),
	"MSBuild.exe",
	[IO.SearchOption]::AllDirectories
) | Sort-Object -Descending | Select-Object -First 1

& $MSBuildPath DataBoss\DataBoss.sln /t:Clean /p:Configuration=Release /v:m /nologo
& $MSBuildPath Build.proj /t:Package /p:Configuration=Release /p:Build=$Build /v:m /nologo

& ./Tools/NuGet.exe pack DataBoss\DataBoss.csproj -OutputDirectory Artifacts
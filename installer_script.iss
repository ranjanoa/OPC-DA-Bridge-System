[Setup]
AppId={{C6D2D4B1-8C3B-4A1B-9D8F-F265E0D28EC9}
AppName=Industrial OPC Bridge
AppVersion=1.0
DefaultDirName={autopf}\IndustrialOpcBridge
DefaultGroupName=Industrial OPC Bridge
OutputDir=.
OutputBaseFilename=OPC_Bridge_Installer
Compression=lzma
SolidCompression=yes
ArchitecturesInstallIn64BitMode=no

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
; IMPORTANT: This looks for files in the 'publish' folder created by the build step
Source: "publish\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\Industrial OPC Bridge"; Filename: "{app}\OpcDaBridge.exe"
Name: "{autodesktop}\Industrial OPC Bridge"; Filename: "{app}\OpcDaBridge.exe"; Tasks: desktopicon

[Run]
Filename: "{app}\OpcDaBridge.exe"; Description: "{cm:LaunchProgram,Industrial OPC Bridge}"; Flags: nowait postinstall skipifsilent
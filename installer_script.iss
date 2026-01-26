[Setup]
AppId={{C6D2D4B1-8C3B-4A1B-9D8F-F265E0D28EC9}
AppName=Industrial OPC Bridge
AppVersion=1.0
DefaultDirName={autopf}\IndustrialOpcBridge
DefaultGroupName=Industrial OPC Bridge
; OutputDir is set to . so the GitHub Action can find the exe in the root
OutputDir=.
OutputBaseFilename=OPC_Bridge_Installer
Compression=lzma
SolidCompression=yes
; We removed ArchitecturesInstallIn64BitMode to default to 32-bit (x86) mode

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
; This looks for the files published by the GitHub Action
Source: "publish\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\Industrial OPC Bridge"; Filename: "{app}\OpcDaBridge.exe"
Name: "{autodesktop}\Industrial OPC Bridge"; Filename: "{app}\OpcDaBridge.exe"; Tasks: desktopicon

[Run]
Filename: "{app}\OpcDaBridge.exe"; Description: "{cm:LaunchProgram,Industrial OPC Bridge}"; Flags: nowait postinstall skipifsilent
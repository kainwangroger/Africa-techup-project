# prepare_submission.ps1
# Ce script cree un dossier 'submission_lite' et une archive ZIP < 100Mo

$destination = "submission_lite"
$zipFile = "Africa_TechUp_Project_Submission.zip"

# 1. Nettoyage si deja existant
if (Test-Path $destination) { Remove-Item -Recurse -Force $destination }
if (Test-Path $zipFile) { Remove-Item -Force $zipFile }

# 2. Creation du dossier temporaire
New-Item -ItemType Directory -Path $destination

# 3. Copie des repertoires essentiels (Liste blanche)
$includes = @("dags", "include", "config", "tests", "plugins", "grafana", "prometheus", "promtail", "postgres")

foreach ($folder in $includes) {
    if (Test-Path $folder) {
        Copy-Item -Path $folder -Destination "$destination\$folder" -Recurse -Container
    }
}

# 4. Copie des fichiers racines essentiels
$rootFiles = @("docker-compose.yaml", "scripts-sql.sql", "README.md", "project_overview.md", "Pitch_Presentation_AfricaTechUp.md", "Dashboards_User_Guide.md", "requirements.txt", ".env.example")

foreach ($file in $rootFiles) {
    if (Test-Path $file) {
        Copy-Item -Path $file -Destination "$destination\$file"
    }
}

# 5. Creation du .env de base
if (Test-Path ".env") {
    Get-Content ".env" | Where-Object { $_ -notmatch "API_KEY|TOKEN|SECRET" } > "$destination\.env.submission"
}

# 6. Archivage ZIP
Compress-Archive -Path "$destination\*" -DestinationPath $zipFile -Force

Write-Host "------------------------------------------------" -ForegroundColor Cyan
Write-Host "TERMINE ! L'archive $zipFile a ete creee." -ForegroundColor Green
Write-Host "Dossier de travail : $destination" -ForegroundColor Yellow
$size = (Get-Item $zipFile).Length / 1MB
Write-Host "Taille finale : $([math]::Round($size, 2)) Mo" -ForegroundColor Green
Write-Host "------------------------------------------------" -ForegroundColor Cyan

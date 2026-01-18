$containerName = "creatorverse-ai-token-metrics-service"
$imageName = "creatorverse-ai-token-metrics-service:1.0.0"

# Parse arguments
$noCache = $args -contains "--no-cache"

Write-Host "Stopping and removing existing container..." -ForegroundColor Yellow
docker rm -f $containerName 2>$null

if ($noCache) {
    Write-Host "Building Docker image (no cache - full rebuild)..." -ForegroundColor Cyan
    docker build --no-cache -t ${imageName} -t creatorverse-ai-token-metrics-service:latest .
} else {
    Write-Host "Building Docker image (with cache)..." -ForegroundColor Cyan
    docker build -t ${imageName} -t creatorverse-ai-token-metrics-service:latest .
}

if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed!" -ForegroundColor Red
    exit 1
}

Write-Host "Starting container..." -ForegroundColor Green
docker run -d --name $containerName -p 8080:8080 $imageName

Write-Host "Container started. Checking status..." -ForegroundColor Cyan
docker ps --filter "name=$containerName"

Write-Host "`nView logs with: docker logs -f $containerName" -ForegroundColor Yellow

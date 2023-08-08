BUILD_DIR=$(pwd)/build
GOOS=linux GOARCH=amd64 go build -o  ${BUILD_DIR}/bin/filebeat_linux_amd64
GOOS=linux GOARCH=arm64 go build -o  ${BUILD_DIR}/bin/filebeat_linux_arm64
mkdir -p /workspace/golang_workspace/cvmart-log-pilot/filebeat-binary/
cp ./build/bin/*  /workspace/golang_workspace/cvmart-log-pilot/filebeat-binary/
echo "success"
echo $(ls /workspace/golang_workspace/cvmart-log-pilot/filebeat-binary/)


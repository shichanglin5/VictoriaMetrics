同程相关配置文件

1、在 vm 项目目录下构建 golang-image-builder 镜像并push到17usoft：
DOCKER_BUILD_OPTS="--platform=linux/amd64 --push" GO_BUILDER_IMAGE=golang:1.23.0-alpine BUILDER_IMAGE=docker.17usoft.com/lhhdz/golang-image-builder make package-builder

2、拉取alpine镜像并 push 到 17usoft
docker pull alpine:3.20.2 && \
docker tag alpine:3.20.2 docker.17usoft.com/lhhdz/alpine:3.20.2 && \
docker push docker.17usoft.com/lhhdz/alpine:3.20.2

3、将 Dockerfile 复制并粘贴到jean构建配置（如果不是构建vmagent的话，需要调整），开始构建镜像
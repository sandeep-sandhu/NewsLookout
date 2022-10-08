
# Running NewsLookout in a container environment
The application has been packaged into a docker container to be used in a development environment or deployed in a Kubernetes cluster.

## Docker Image
The container image can be built using the provided Dockerfile in this repository. It can be built using the `docker build` command.

This image has been built based on a Ubuntu Linux base image, but any Linux image could be used for building this.

Since the application is written purely in Python, the image could very well have been built on a Windows base image as well. 

## Example of a Docker Container
An example deployment has also been provided in a docker-compose YAML file. this can be further customised to your respective environment and use case.

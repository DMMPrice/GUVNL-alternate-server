pipeline {
    agent any

    environment {
        IMAGE_NAME = "dmmprice/guvnl-alternate-server"
        CONTAINER_NAME = "guvnl-alternate-server"
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build Docker Image') {
            steps {
                script {
                    sh """
                      echo "Building Docker image..."
                      docker build -t ${IMAGE_NAME}:${BUILD_NUMBER} -t ${IMAGE_NAME}:latest .
                    """
                }
            }
        }

        stage('Push to Docker Hub') {
            steps {
                script {
                    withCredentials([usernamePassword(
                        credentialsId: 'dockerhub-dmmprice',
                        usernameVariable: 'DOCKER_USER',
                        passwordVariable: 'DOCKER_PASS'
                    )]) {
                        sh """
                          echo "$DOCKER_PASS" | docker login -u "$DOCKER_USER" --password-stdin
                          docker push ${IMAGE_NAME}:latest
                        """
                    }
                }
            }
        }

        stage('Deploy Container on VPS') {
            steps {
                script {
                    withCredentials([file(credentialsId: 'guvnl-alt-env-file', variable: 'ENV_FILE')]) {
                        sh """
                          echo "Pulling latest image..."
                          docker pull ${IMAGE_NAME}:latest

                          echo "Stopping old container if exists..."
                          docker stop ${CONTAINER_NAME} || true
                          docker rm ${CONTAINER_NAME} || true

                          echo "Starting new container on port 4002..."
                          docker run -d \\
                            --name ${CONTAINER_NAME} \\
                            -p 4002:4000 \\
                            --env-file "$ENV_FILE" \\
                            ${IMAGE_NAME}:latest
                        """
                    }
                }
            }
        }
    }

    post {
        always {
            sh 'docker image prune -f || true'
        }
    }
}

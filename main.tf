# main.tf
provider "aws" {
  region = "us-west-2"  # Adjust as needed
}

resource "aws_instance" "mlops_instance" {
  ami           = "ami-0c55b159cbfafe1f0"  # Amazon Linux 2 AMI (update for your region)
  instance_type = "t3.xlarge"              # 4 vCPUs, 16GB RAM
  key_name      = "your-key-pair"          # Replace with your SSH key pair name
  security_groups = [aws_security_group.mlops_sg.name]

  user_data = <<-EOF
              #!/bin/bash
              yum update -y
              # Install Docker
              yum install -y docker
              systemctl start docker
              systemctl enable docker
              usermod -aG docker ec2-user
              # Install Minikube prerequisites
              curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
              install minikube-linux-amd64 /usr/local/bin/minikube
              # Install kubectl
              curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
              chmod +x kubectl
              mv kubectl /usr/local/bin/
              # Install Terraform (optional, for local execution)
              yum install -y unzip
              curl -LO https://releases.hashicorp.com/terraform/1.6.6/terraform_1.6.6_linux_amd64.zip
              unzip terraform_1.6.6_linux_amd64.zip
              mv terraform /usr/local/bin/
              EOF

  tags = {
    Name = "MLOps-Minikube-Instance"
  }
}

resource "aws_security_group" "mlops_sg" {
  name        = "mlops_sg"
  description = "Security group for MLOps instance"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Restrict to your IP in production
  }

  ingress {
    from_port   = 8443  # Minikube API server
    to_port     = 8443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

output "instance_public_ip" {
  value = aws_instance.mlops_instance.public_ip
}
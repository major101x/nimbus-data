

## How to deploy
```
docker build --target producer -t producer:dev .
docker build --target consumer -t consumer:dev .
kind load docker-image producer:dev consumer:dev --name c1 

kubectl apply -f k8s/producer-deployment.yaml -f k8s/consumer-deployment.yaml
kubectl rollout restart deploy/producer deploy/consumer -n strimzi

kubectl scale deploy/producer -n strimzi --replicas=1
kubectl scale deploy/consumer -n strimzi --replicas=1
``
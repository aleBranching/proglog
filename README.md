

running a jaegar container:
docker run --name jaeger   -e COLLECTOR_OTLP_ENABLED=true   -p 16686:16686   -p 4317:4317   jaegertracing/all-in-one:lateste



This contains the best explanation of opentelemetry I've encountered
https://www.romaglushko.com/blog/opentelemetry-sdk/




## exporting traces to jaeger
![alt text](image.png)
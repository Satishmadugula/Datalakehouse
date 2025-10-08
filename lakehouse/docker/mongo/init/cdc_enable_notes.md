Enable change streams by ensuring the MongoDB replica set is initialized. The docker-compose deployment starts MongoDB in standalone replica set mode. To verify:

```
docker exec -it mongo mongosh --eval 'rs.status()'
```

If not initialized, run:

```
docker exec -it mongo mongosh --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"mongo:27017"}]})'
```

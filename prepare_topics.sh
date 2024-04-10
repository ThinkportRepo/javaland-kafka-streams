echo "deleting credit-rating-insufficient"
kafka-topics --bootstrap-server localhost:9092 --delete --topic credit-rating-insufficient
echo "deleting new-insurance-request"
kafka-topics --bootstrap-server localhost:9092 --delete --topic new-insurance-request
echo "deleting insurance-concluded"
kafka-topics --bootstrap-server localhost:9092 --delete --topic insurance-concluded
echo "deleting credit-rating-result"
kafka-topics --bootstrap-server localhost:9092 --delete --topic credit-rating-result

echo "creating credit-rating-insufficient"
kafka-topics --bootstrap-server localhost:9092 --create --topic credit-rating-insufficient --partitions 1 --replication-factor 1
echo "creating new-insurance-request"
kafka-topics --bootstrap-server localhost:9092 --create --topic new-insurance-request --partitions 1 --replication-factor 1
echo "creating insurance-concluded"
kafka-topics --bootstrap-server localhost:9092 --create --topic insurance-concluded --partitions 1 --replication-factor 1
echo "creating credit-rating-result"
kafka-topics --bootstrap-server localhost:9092 --create --topic credit-rating-result --partitions 1 --replication-factor 1

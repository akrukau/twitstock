#!/usr/bin/env bash

dns_names=(ec2-52-34-147-146.us-west-2.compute.amazonaws.com \
    ec2-52-88-73-44.us-west-2.compute.amazonaws.com \
    ec2-52-34-140-102.us-west-2.compute.amazonaws.com \
    ec2-52-88-87-17.us-west-2.compute.amazonaws.com)

cd /usr/local/
sudo git clone https://github.com/linkedin/camus.git
sudo chown -R ubuntu camus
cd ./camus/
mvn clean package

rm -f ./configure-camus.py
cat << EOF >> ./configure-camus.py
import sys
if len(sys.argv) != 3:
    sys.exit()
filename = sys.argv[1]
input = open(filename, "r")
for line in input:
    if line.startswith("etl.destination.path"):
        new_line = "etl.destination.path=hdfs://" + sys.argv[2] + ":9000/camus/topics"
    elif line.startswith("etl.execution.base.path"):
        new_line = "etl.destination.path=hdfs://" + sys.argv[2] + ":9000/camus/exec" 
    elif line.startswith("etl.execution.history.path"):
        new_line = "etl.destination.path=hdfs://" + sys.argv[2] + ":9000/camus/exec/history" 
    elif line.startswith("camus.message.decoder.class"):
        new_line = "camus.message.decoder.class=com.linkedin.camus.etl.kafka.coders" + \
            ".JsonStringMessageDecoder\n"  + \
            "etl.record.writer.provider.class=com.linkedin.camus.etl.kafka.common.StringRecordWriterProvider"
    elif line.startswith("etl.output.codec"):
        new_line = "etl.output.codec=gzip" 
    else:
        new_line = line.rstrip()
    print new_line

broker_info = "kafka.brokers=ec2-52-34-147-146.us-west-2.compute.amazonaws.com:9092," + \
    "ec2-52-88-73-44.us-west-2.compute.amazonaws.com:9092," + \
    "ec2-52-34-140-102.us-west-2.compute.amazonaws.com:9092," \
    "ec2-52-88-87-17.us-west-2.compute.amazonaws.com:9092"
    
print broker_info

EOF

camus_properties=/usr/local/camus/camus-example/src/main/resources/camus.properties
python ./configure-camus.py $camus_properties ${dns_names[0]} > new_camus.properties
mv new_camus.properties $camus_properties


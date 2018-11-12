#!/bin/bash
rm -rf cache
rm -rf result
rm -rf output
spark-submit --jars stanford-corenlp-3.8.0.jar,jsoup-1.10.3.jar,guava-14.0.1.jar --driver-memory 36g --executor-memory 36g --class ch.epfl.dlab.quootstrap.QuotationExtraction target/quootstrap-0.0.2-make.jar output

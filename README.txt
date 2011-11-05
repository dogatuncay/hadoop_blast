Write the instruction(s) for running your program here


  


(1) Downlaod BlastProgramAndDB.tar.gz and specified version of hadoop, untar hadoop tar ball.

(2) Start Hadoop and HDFS with Single or Multiple starting scripts provided by AI.

(3) Upload relevant files into HDFS. Basically like this:

./hadoop fs -put ~/Hadoop-Blast/input HDFS_blast_input
./hadoop fs -ls HDFS_blast_input
./hadoop fs -copyFromLocal $BLAST_HOME/BlastProgramAndDB.tar.gz BlastProgramAndDB.tar.gz
./hadoop fs -ls BlastProgramAndDB.tar.gz

(4) Run Blast hadoop with the following command:
cd ~/hadoop/bin
./hadoop jar ~/Hadoop-Blast/executable/blast-hadoop.jar BlastProgramAndDB.tar.gz bin/blastx /tmp/hadoop-test/ db nr HDFS_blast_input HDFS_blast_output '-query #_INPUTFILE_# -outfmt 6 -seg no -out #_OUTPUTFILE_#'

(5) Collect all output and archive them into two single files result.fa and errorResult.fa.
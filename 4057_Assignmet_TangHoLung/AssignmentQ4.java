import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AssignmentQ4 {

    public static class bibleMapper
            extends Mapper<LongWritable, Text, Text, Line> {

        private Text bKey = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            //valuesable library
            String lines = value.toString();     //take all lines in bible
            ArrayList<Line> arrays = new ArrayList<Line>(); //arraylist to save each book one by one

            //split the bible line by line
            StringTokenizer tokenizer = new StringTokenizer(lines, "\n");

            //if having next line, do the split to split the book,chapter,verse,content
            try {
                while (tokenizer.hasMoreTokens()) {
                    Line line = new Line();
                    String bookName = "";

                    //word = current line
                    bKey.set(tokenizer.nextToken());

                    //e.g "1 Chronicles 8:7 And Naaman, and Ahiah, and Gera, he removed them, and begat Uzza, and Ahihud."
                    //1 Chronicles 8:7[0] And Naaman, and Ahiah, and Gera, he removed them, and begat Uzza, and Ahihud.[1]
                    bKey.toString().split("\t");

                    //And Naaman, and Ahiah, and Gera, he removed them, and begat Uzza, and Ahihud.
                    line.setContent(bKey.toString().split("\t")[1]);

                    //1 Chronicles 8:7  *size-1 = book name
                    String[] arr = bKey.toString().split("\t")[0].split(" ");

                    //1[0] Chronicles[1] 8:7[2]
                    for (int i = 0; i < arr.length-1 ; i++){
                        //1 Chronicles
                        if (i == 0){
                            bookName += arr[i];
                        }else{
                            bookName += " "+arr[i];
                        }
                    }
                    line.setBook(bookName);

                    //8
                    line.setChapter(arr[arr.length-1].split(":")[0]);
                    bKey.set(line.getChapter());

                    //7
                    line.setVerse(arr[arr.length-1].split(":")[1]);

                    //add the object to the arraylist
                    arrays.add(line);

                    //send the book name and Line Object to reducer
                    context.write(new Text(bookName), line);

                }
            }catch (Exception e) {
                System.out.println("Error");
                e.printStackTrace();
            }

        }
    }

    public static class bibleReducer
            extends Reducer<Text, Line, Text, Text> {
        Line line = new Line();

        //sort the item first, only same things shown
        public void reduce(Text key, Iterable<Line> values, Context context
        ) throws IOException, InterruptedException {
            String chapter = "";
            String verse = "";
            int minValue = 0;
            boolean isTrue = false;

            for (Line line : values) {
                //get the content from the line object and split it by " "
                String content = line.getContent();
                //"And" "Naaman," "and" "Ahiah," "and" "Gera," "he" "removed" "them," "and" "begat" "Uzza," "and" "Ahihud."
                String[] contentWords = content.split(" ");

                //if this is the first time of the loop, set the minValue as first contentWords, if this e.g. it will be 14
                if(!isTrue){
                    minValue = contentWords.length;
                    isTrue = true;
                }

                //this for loop is used the check which verse is the shortest verse
                for(int i=1;i<contentWords.length;i++){
                    if(contentWords.length < minValue){
                        minValue = contentWords.length;
                        chapter = line.getChapter();
                        verse = line.getVerse();
                    }
                }
            }
            Text t = new Text("Book :"+ key);
            Text t2 = new Text(chapter+":"+verse+" is the shortest verse having "+ minValue +" words");
            context.write(t, t2);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Bible <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "AssignmentQ4 by TangHoLung 21220670");
        job.setJarByClass(AssignmentQ4.class);
        job.setMapperClass(bibleMapper.class);
        job.setReducerClass(bibleReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Line.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Line implements Writable {
        private String book;
        private String chapter;
        private String verse;
        private String content;
        private String line;

        public Line(String book, String chapter, String verse, String content) {
            this.book = book;
            this.chapter = chapter;
            this.verse = verse;
            this.content = content;
        }

        public Line() {}

        public String getBook() {return book;}
        public String getChapter() {return chapter;}
        public String getVerse() {return verse;}
        public String getContent() {return content;}

        public void setBook(String book) {this.book = book;}
        public void setChapter(String chapter) {this.chapter = chapter;}
        public void setVerse(String verse) {this.verse = verse;}
        public void setContent(String content) {this.content = content;}

        public void write(DataOutput out) throws IOException{
            out.writeUTF(book);
            out.writeUTF(chapter);
            out.writeUTF(verse);
            out.writeUTF(content);
        };
        public void readFields(DataInput in) throws IOException{
            book = in.readUTF();
            chapter = in.readUTF();
            verse = in.readUTF();
            content = in.readUTF();
        };
    }
}
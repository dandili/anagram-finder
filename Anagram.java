import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Anagram {

    public static class AnagramMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text word = new Text();
        private final Text sortedWord = new Text();

        public void map(LongWritable key, Text value, Context context
                        ) throws IOException, InterruptedException {

            String[] stopWords = {"'tis","'twas","a","able","about","across","after","ain't","all","almost","also","am","among","an","and","any","are","aren't","as","at","be","because","been","but","by","can","can't","cannot","could","could've","couldn't","dear","did","didn't","do","does","doesn't","don't","either","else","ever","every","for","from","get","got","had","has","hasn't","have","he","he'd","he'll","he's","her","hers","him","his","how","how'd","how'll","how's","however","i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","just","least","let","like","likely","may","me","might","might've","mightn't","most","must","must've","mustn't","my","neither","no","nor","not","of","off","often","on","only","or","other","our","own","rather","said","say","says","shan't","she","she'd","she'll","she's","should","should've","shouldn't","since","so","some","than","that","that'll","that's","the","their","them","then","there","there's","these","they","they'd","they'll","they're","they've","this","tis","to","too","twas","us","wants","was","wasn't","we","we'd","we'll","we're","were","weren't","what","what'd","what's","when","when","when'd","when'll","when's","where","where'd","where'll","where's","which","while","who","who'd","who'll","who's","whom","why","why'd","why'll","why's","will","with","won't","would","would've","wouldn't","yet","you","you'd","you'll","you're","you've","your"};
            String[] split = value.toString().split("[-.,:/]");     // Break the string based on specific string delimiters

            // For every string in the split array
            for (int x = 0; x < split.length; x++) {
                StringTokenizer itr = new StringTokenizer(value.toString());

                // While there are more words
                while (itr.hasMoreTokens()) {
                    if (StopWords(itr.toString(), stopWords)) { // Check to see if word is a stop word
                        String token = itr.nextToken().trim().replaceAll("[^a-zA-Z]", "").toLowerCase(); // Make all chars lowercase
                        word.set(token);    // Set token to word
                        sortedWord.set(alphabeticalSort(token)); // Code would throw error in block so function was made public out of the class
                        context.write(sortedWord, word);
                    }
                }
            }
        }

        // Search for the StopWords in an Array
        public static boolean StopWords(String in, String[] arr) {
            for (int i = 0; i<arr.length; i++){
                if (in.contains(arr[i])) {
                    return true;
                }
            } return false;
        }

        // Sort string alphabetically
        public String alphabeticalSort(String in) {
            char[] chars = in.toCharArray();    // Returns an array of chars after converting a String into sequence of characters
            Arrays.sort(chars);                 // Method sorts the elements of an array and returns the sorted array alphabetically
            String out = new String(chars);
            return out;
        }

        /*
        public String removeAllStopWords(String[] in) {
            ArrayList<String> allWords =
                    Stream.of(in)
                            .collect(Collectors.toCollection(ArrayList<String>::new));
            allWords.removeAll(stopWords);
            return allWords.stream().collect(Collectors.joining(""));
        }
         */
    }


    public static class AnagramReducer extends Reducer<Text, Text, Text, Text> {

        private final Text outValue = new Text();
        private final Text outKey = new Text();
        String result = "";
        int anagramTally = 1;

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Set<Text> uniqueWords = new HashSet<Text>();
            Map<String, Integer> anagramWords = new TreeMap<String, Integer>();

            /*
              TreeMap automatically sorts the words
              in alphabetical order but has many incompat issues
              with <Text> type. So used String and used Integer to count the frequency.
             */
            int wordSum = 0;
            StringBuilder words = new StringBuilder().append("[");
            for (Text val : values) { //for each value
                String valueToString = val.toString();

                // Standard output of anagrams
                if (uniqueWords.add(val)) { // if the value is not a duplicate of one before
                    words.append(val.toString()); // add the string to the builder
                    words.append(", "); // Add a comma to divide the anagrams
                    wordSum++;
                }

                /* Calculating word frequency
                    Uncomment bottom if statement and comment top if to calculate word frequency.
                 */
                if (valueToString.length()>1) { // Omits single letter words
                    if(anagramWords.containsKey(valueToString)) { // Checks to see if the anagram words contain the value
                        anagramWords.put(valueToString, anagramWords.get(valueToString)+1); // If so it finds the string and increments the occurrences of that string value
                    } else {
                        anagramWords.put(valueToString, 1); // Otherwise it adds the anagram word and starts the counter
                    }
                }
            }
            words.setLength(words.length() - 2); //remove the comma from the end
            result = "# of anagrams"+uniqueWords.size();
            words.append("] \t" + result); // Append text and concat the number of anagrams
            StringBuilder builderKey = new StringBuilder();


            /* Basic anagram with simple output, uncomment and comment below block to calculate both
            if (wordSum > 1) {
                outValue.set(words.toString()); // output the builder string containing all anagrams as value
                outKey.set(builderKey.toString()); //set the output key as nothing as it is not needed
                context.write(outKey, outValue);
            }*/


            // Word frequency code and alphabetically sorted within the anagram set, uncomment and comment above block to calculate both.
            if (anagramWords.size() > 1) {
                outValue.set(anagramWords.toString()); // Output the anagram TreeMap string containing all anagrams as value and uses a counter
                outKey.set(Integer.toString(anagramTally)); // Set the output key as the sum of the occurrences of the integer
                anagramTally++;
                context.write(outKey, outValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "anagram");
        job.setJarByClass(Anagram.class);
        job.setMapperClass(AnagramMapper.class);
        job.setReducerClass(AnagramReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

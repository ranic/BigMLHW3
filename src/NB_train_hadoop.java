import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;

/**
 * Created by vijay on 1/12/15.
 */
public class NB_train_hadoop {
    private static final String[] labels = {"ECAT", "GCAT", "CCAT", "MCAT"};
    private static final Set<String> validLabels = new HashSet<String>(Arrays.asList(labels));
    //private static final String[] STOP_WORDS = {"a","about","above","across","after","again","against","all","almost","alone","along","already","also","although","always","among","an","and","another","any","anybody","anyone","anything","anywhere","are","area","areas","around","as","ask","asked","asking","asks","at","away","b","back","backed","backing","backs","be","became","because","become","becomes","been","before","began","behind","being","beings","best","better","between","big","both","but","by","c","came","can","cannot","case","cases","certain","certainly","clear","clearly","come","could","d","did","differ","different","differently","do","does","done","down","down","downed","downing","downs","during","e","each","early","either","end","ended","ending","ends","enough","even","evenly","ever","every","everybody","everyone","everything","everywhere","f","face","faces","fact","facts","far","felt","few","find","finds","first","for","four","from","full","fully","further","furthered","furthering","furthers","g","gave","general","generally","get","gets","give","given","gives","go","going","good","goods","got","great","greater","greatest","group","grouped","grouping","groups","h","had","has","have","having","he","her","here","herself","high","high","high","higher","highest","him","himself","his","how","however","i","if","important","in","interest","interested","interesting","interests","into","is","it","its","itself","j","just","k","keep","keeps","kind","knew","know","known","knows","l","large","largely","last","later","latest","least","less","let","lets","like","likely","long","longer","longest","m","made","make","making","man","many","may","me","member","members","men","might","more","most","mostly","mr","mrs","much","must","my","myself","n","necessary","need","needed","needing","needs","never","new","new","newer","newest","next","no","nobody","non","noone","not","nothing","now","nowhere","number","numbers","o","of","off","often","old","older","oldest","on","once","one","only","open","opened","opening","opens","or","order","ordered","ordering","orders","other","others","our","out","over","p","part","parted","parting","parts","per","perhaps","place","places","point","pointed","pointing","points","possible","present","presented","presenting","presents","problem","problems","put","puts","q","quite","r","rather","really","right","right","room","rooms","s","said","same","saw","say","says","second","seconds","see","seem","seemed","seeming","seems","sees","several","shall","she","should","show","showed","showing","shows","side","sides","since","small","smaller","smallest","so","some","somebody","someone","something","somewhere","state","states","still","still","such","sure","t","take","taken","than","that","the","their","them","then","there","therefore","these","they","thing","things","think","thinks","this","those","though","thought","thoughts","three","through","thus","to","today","together","too","took","toward","turn","turned","turning","turns","two","u","under","until","up","upon","us","use","used","uses","v","very","w","want","wanted","wanting","wants","was","way","ways","we","well","wells","went","were","what","when","where","whether","which","while","who","whole","whose","why","will","with","within","without","work","worked","working","works","would","x","y","year","years","yet","you","young","younger","youngest","your","yours","z"};
    //private static final Set<String> STOP_WORD_SET = new HashSet<String>(Arrays.asList(STOP_WORDS));


    public static class TokenizerMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        private List<String> filterLabels(String[] labels) {
            ArrayList<String> result = new ArrayList<String>();
            for (String label: labels) {
                if (validLabels.contains(label)) {
                    result.add(label);
                }
            }
            return result;
        }

        private Vector<String> tokenize(String cur_doc) {
            String[] words = cur_doc.split("\\s+");
            Vector<String> tokens = new Vector<String>();
            for (int i = 0; i < words.length; i++) {
                words[i] = words[i].replaceAll("\\W", "");
                if (words[i].length() > 0) {
                    tokens.add(words[i]);
                }
            }
            return tokens;
        }

        @Override
        public void map(LongWritable longWritable, Text value,
                        OutputCollector<Text, IntWritable> context,
                        Reporter reporter) throws IOException {
            String[] pair = value.toString().split("\t");
            List<String> labels = filterLabels(pair[0].split(","));
            Vector<String> tokens = tokenize(pair[1]);

            // #(Y = *) += #(labels)
            context.collect(new Text("Y=*"), new IntWritable(labels.size()));

            for (String label : labels) {
                // #(Y = y) += 1
                context.collect(new Text("Y=" + label), one);

                // #(Y = y, W = *) += |document|
                context.collect(new Text("Y=" + label + ",W=*"), new IntWritable(tokens.size()));

                for (String token : tokens) {
                    // #(Y = y, W = w) += 1
                    context.collect(new Text("Y=" + label + ",W=" + token), one);
                }
            }
        }
    }

    public static class IntSumReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> context,
                           Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            context.collect(key, new IntWritable(sum));
        }
    }
}

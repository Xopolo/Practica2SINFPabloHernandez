package es.unex.cum.sinf.practica3.pab_her.Consulta4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * The type Consulta 4 reducer.
 */
public class Consulta4Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        final DecimalFormat decimalFormat = new DecimalFormat("#.##");

        int num_viento = 0;

        Double sum_viento = 0d;


        for (Text iter : values) {
            String[] cads = iter.toString().split(";");
            Double[] doubles = new Double[cads.length];
            for (int i = 0; i < cads.length; i++) {
                if (cads[i].equals("null")) {
                    doubles[i] = null;
                    continue;
                }
                doubles[i] = Double.parseDouble(cads[i].replace(",", "."));
            }
            if (doubles[0] != null) {
                num_viento++;
                sum_viento += doubles[0];
            }
        }

        context.write(key, new Text(
                "Velocidad del viento media\t" +
                        decimalFormat.format(sum_viento / num_viento) + " m/s"));
    }
}

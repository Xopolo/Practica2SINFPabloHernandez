package es.unex.cum.sinf.practica3.pab_her.Consulta7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * The type Consulta 7 reducer.
 */
public class Consulta7Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        final DecimalFormat decimalFormat = new DecimalFormat("#.##");

        double sum_precipitacion = 0;
        int num_precipitacion = 0;

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
                num_precipitacion++;
                sum_precipitacion += doubles[0];
            }
        }


        context.write(key, new Text(
                "Precipitación media:\t" + decimalFormat.format(sum_precipitacion / num_precipitacion) + " mm"
        ));

    }
}

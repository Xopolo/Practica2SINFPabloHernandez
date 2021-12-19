package es.unex.cum.sinf.practica3.pab_her.Consulta5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * The type Consulta 5 reducer.
 */
public class Consulta5Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        final DecimalFormat decimalFormat = new DecimalFormat("#.##");

        double max_tmp = Double.MIN_VALUE, min_tmp = Double.MAX_VALUE;
        double max_hum = Double.MIN_VALUE, min_hum = Double.MAX_VALUE;
        double max_pre = Double.MIN_VALUE, min_pre = Double.MAX_VALUE;

        double rad_max = Double.MIN_VALUE;
        double rad_min = Double.MAX_VALUE;

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
            if (doubles[2] != null) {
                if (max_tmp < doubles[2]) max_tmp = doubles[2];
                if (min_tmp > doubles[2]) min_tmp = doubles[2];
            }
            if (doubles[3] != null) {
                if (max_hum < doubles[3]) max_hum = doubles[3];
                if (min_hum > doubles[3]) min_hum = doubles[3];
            }
            if (doubles[0] != null) {
                if (max_pre < doubles[0]) max_pre = doubles[0];
                if (min_pre > doubles[0]) min_pre = doubles[0];
            }
            if (doubles[1] != null) {
                if (rad_max < doubles[1]) rad_max = doubles[1];
                if (rad_min > doubles[1]) rad_min = doubles[1];
            }
        }

        context.write(key, new Text(
                "Temperatura maxima\t" +
                        decimalFormat.format(max_tmp) + "ºC\t\t" +
                        "Temperatura minima\t" +
                        decimalFormat.format(min_tmp) + "ºC\t\t" +
                        "Humedad maxima\t" +
                        decimalFormat.format(max_hum) + "%\t\t" +
                        "Humedad minima\t" +
                        decimalFormat.format(min_hum) + "%\t\t" +
                        "Presion maxima\t" +
                        decimalFormat.format(max_pre) + "(mb)\t\t" +
                        "Presion minima\t" +
                        decimalFormat.format(min_pre) + "(mb)\t\t" +
                        "Radiacion maxima\t" +
                        decimalFormat.format(rad_max) + "(KJ/m2)\t\t" +
                        "Radiacion minima\t" +
                        decimalFormat.format(rad_min) + "(KJ/m2)"));
    }
}

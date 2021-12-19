package es.unex.cum.sinf.practica3.pab_her.Consulta3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * The type Consulta 3 reducer.
 */
public class Consulta3Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        final DecimalFormat decimalFormat = new DecimalFormat("#.##");

        int num_presion = 0, num_tmp = 0, num_hum = 0;

        Double sum_presion_atm = 0d;
        Double sum_temperatura = 0d;
        Double sum_humedad = 0d;


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
                num_presion++;
                sum_presion_atm += doubles[0];
            }
            if (doubles[1] != null) {
                num_tmp++;
                sum_temperatura += doubles[1];
            }
            if (doubles[2] != null) {
                num_hum++;
                sum_humedad += doubles[2];
            }

        }

        context.write(key, new Text(
                "Presión media    " +
                        decimalFormat.format(sum_presion_atm / num_presion) + "(mb)\t\t" +
                        "Temperatura media    " +
                        decimalFormat.format(sum_temperatura / num_tmp) + "ºC\t\t" +
                        "Humedad media    " +
                        decimalFormat.format(sum_humedad / num_hum) + "%"));

    }
}

package es.unex.cum.sinf.practica3.pab_her.Consulta2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * The type Consulta 2 reducer.
 */
public class Consulta2Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        final DecimalFormat decimalFormat = new DecimalFormat("#.##");

        int num_presion = 0, num_radiacion = 0, num_tmp = 0, num_hum = 0, num_viento = 0;

        Double sum_presion_atm = 0d;
        Double sum_radiacion_solar = 0d;
        Double sum_temperatura = 0d;
        Double sum_humedad = 0d;
        Double sum_velocidad_viento = 0d;


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
                num_radiacion++;
                sum_radiacion_solar += doubles[1];
            }
            if (doubles[2] != null) {
                num_tmp++;
                sum_temperatura += doubles[2];
            }
            if (doubles[3] != null) {
                num_hum++;
                sum_humedad += doubles[3];
            }
            if (doubles[4] != null) {
                num_viento++;
                sum_velocidad_viento += doubles[4];
            }
        }

        context.write(key, new Text(
                        "\t\tPresión media \t" +
                        decimalFormat.format(sum_presion_atm / num_presion) + "(mb)\t\t" +
                        "Radiacion solar media \t" +
                        decimalFormat.format(sum_radiacion_solar / num_radiacion) + "(KJ/m2)\t\t" +
                        "Temperatura media \t" +
                        decimalFormat.format(sum_temperatura / num_tmp) + "ºC\t\t" +
                        "Humedad media \t" +
                        decimalFormat.format(sum_humedad / num_hum) + "%\t\t" +
                        "Velocidad viento media \t" +
                        decimalFormat.format(sum_velocidad_viento / num_viento) + " m/s"));

    }
}

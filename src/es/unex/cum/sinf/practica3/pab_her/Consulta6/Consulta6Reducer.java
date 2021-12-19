package es.unex.cum.sinf.practica3.pab_her.Consulta6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * The type Consulta 6 reducer.
 */
public class Consulta6Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        Double max_precipitacion = Double.MIN_VALUE;
        Double min_precipitacion = Double.MAX_VALUE;
        String fecha_min = null;
        String fecha_max = null;
        String region_min = null;
        String region_max = null;
        String estacion_min = null;
        String estacion_max = null;


        for (Text iter : values) {
            String[] cads = iter.toString().split(";");
            for (int i = 0; i < cads.length; i++) {
                if (cads[i].equals("null")) {
                    cads[i] = null;
                }
            }
            Double precipitacion;

            try {
                precipitacion = Double.parseDouble(cads[3].replace(",", "."));
            } catch (NumberFormatException e) {
                continue;
            }

            if (precipitacion > max_precipitacion) {
                fecha_max = cads[0];
                region_max = cads[1];
                estacion_max = cads[2];
                max_precipitacion = precipitacion;
            } else if (precipitacion < min_precipitacion) {
                fecha_min = cads[0];
                region_min = cads[1];
                estacion_min = cads[2];
                min_precipitacion = precipitacion;
            }
        }

        context.write(key, new Text(
                "\nPrecipitacion MAX:\t" +
                        "Valor:\t" + max_precipitacion + " mm\t\t" +
                        "Fecha:\t" + fecha_max + "\t\t" +
                        "Region:\t" + region_max + "\t\t" +
                        "Estacion:\t" + estacion_max + "\t\t" +
                        "\nPrecipitacion MIN:\t" +
                        "Valor:\t" + min_precipitacion + " mm\t\t" +
                        "Fecha:\t" + fecha_min + "\t\t" +
                        "Region:\t" + region_min + "\t\t" +
                        "Estacion:\t" + estacion_min
        ));

    }
}

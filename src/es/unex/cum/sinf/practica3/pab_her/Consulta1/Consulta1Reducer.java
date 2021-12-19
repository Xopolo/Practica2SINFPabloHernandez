package es.unex.cum.sinf.practica3.pab_her.Consulta1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * The type Consulta 1 reducer.
 */
public class Consulta1Reducer extends Reducer<Text, Text, Text, Text> {

//    @Override
//    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//
//        final DecimalFormat decimalFormat = new DecimalFormat("#.##");
//
//        double num_medidas_temp = 0, num_medidas_hum = 0, num_medidas_presion = 0;
//        Double sum_temp = 0d, sum_hum = 0d, sum_presion = 0d;
//        double rad_max = Double.MIN_VALUE;
//        double rad_min = Double.MAX_VALUE;
//
//        for (Text iter : values) {
//            String[] cads = iter.toString().split(";");
//            Double[] doubles = new Double[cads.length];
//            for (int i = 0; i < cads.length; i++) {
//                if (cads[i].equals("null")) {
//                    doubles[i] = null;
//                    continue;
//                }
//                doubles[i] = Double.parseDouble(cads[i].replace(",", "."));
//            }
//            if (doubles[0] != null) {
//                num_medidas_temp++;
//                sum_temp += doubles[0];
//            }
//
//            if (doubles[1] != null) {
//                num_medidas_hum++;
//                sum_hum += doubles[1];
//            }
//
//            if (doubles[2] != null) {
//                num_medidas_presion++;
//                sum_presion += doubles[2];
//            }
//            if (doubles[3] != null) {
//                if (rad_max < doubles[3]) rad_max = doubles[3];
//                if (rad_min > doubles[3]) rad_min = doubles[3];
//            }
//        }
//
//        if (num_medidas_temp > 0 && num_medidas_hum > 0 && num_medidas_presion > 0) {
//            context.write(key, new Text(decimalFormat.format(sum_temp / num_medidas_temp) + "\t\t" +
//                    decimalFormat.format(sum_hum / num_medidas_hum) + "%\t\t" +
//                    decimalFormat.format(sum_presion / num_medidas_presion) + "\t\t" + decimalFormat.format(rad_max) +
//                    "\t\t" + decimalFormat.format(rad_min)));
//        }
//    }

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
            if (doubles[0] != null) {
                if (max_tmp < doubles[0]) max_tmp = doubles[0];
                if (min_tmp > doubles[0]) min_tmp = doubles[0];
            }

            if (doubles[1] != null) {
                if (max_hum < doubles[1]) max_hum = doubles[1];
                if (min_hum > doubles[1]) min_hum = doubles[1];
            }

            if (doubles[2] != null) {
                if (max_pre < doubles[2]) max_pre = doubles[2];
                if (min_pre > doubles[2]) min_pre = doubles[2];
            }
            if (doubles[3] != null) {
                if (rad_max < doubles[3]) rad_max = doubles[3];
                if (rad_min > doubles[3]) rad_min = doubles[3];
            }
        }

        context.write(key, new Text(
                "Temperatura max:\t" +
                        decimalFormat.format(max_tmp) + "ºC\t\t" +
                        "Temperatura min:\t" +
                        decimalFormat.format(min_tmp) + "ºC\t\t" +
                        "Humedad max:\t" +
                        decimalFormat.format(max_hum) + "%\t\t" +
                        "Humedad min:\t" +
                        decimalFormat.format(min_hum) + "%\t\t" +
                        "Presion max:\t" +
                        decimalFormat.format(max_pre) + "(mb)\t\t" +
                        "Presion min:\t" +
                        decimalFormat.format(min_pre) + "(mb)\t\t" +
                        "Radiacion max:\t" +
                        decimalFormat.format(rad_max) + "(KJ/m2)\t\t" +
                        "Radiacion min:\t" +
                        decimalFormat.format(rad_min) + "(KJ/m2)"));
    }
}

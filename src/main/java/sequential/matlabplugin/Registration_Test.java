package sequential.matlabplugin;

import registeration.Class1;

public class Registration_Test {

    /**
     * registeration() function takes 4 arguments:
     *  path-to-csv-file,
     *  transformation(1,2,3,4),
     *  sequential/parallel(1/2),
     *  type of registration(1-space/2-timelaps)
     *  mu
     */
    public static void main(String[] args) throws Exception {
        Class1 regist = new Class1();
//        regist.registeration("/Users/RundongL/Downloads/RegistrationJava/LiMBP_Neocortical2_StackList.csv",
//                1, 1, 1, 1020);
        regist.registeration(args[0],
                Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]),
                Double.parseDouble(args[4]));
        System.out.println("====> SUCCESS <====");
    }
}

Did you read the log and try to figure it out?
 
I went through the log, the run completed successfully, — all steps (initialization, DB connection, status validation, and update) executed without any errors or warnings.
 
Is there specific config I can validate for this ?
 
Kangude, Rushant
Omkar, Suresh B - I’ve validated the configuration { "PS_DOC_PRODUCT_CODE": "QV,EVSB" } across most of the jobs, and it is present in all of them.
We are getting feedback that this is not true. (in UAT). Please can someone help with this?
 
is anyone using the UR query script to check for the QV?
 
don't know why I ask; here, briefly is how you use it, for whoever is working on this:
 
$ which ur-support-query

ur-support-query () {

        ssh -nq dt77295@edhlsurbd007 "set -x ; /dsto/sw/qa/urtools/system/bin/ur-support-query -filterNoOfResults 0 ${(q)@}"

}
 
$ ur-support-query -stage prod -filterByDays 20 -queryByFilename '*xceleng.xcelstmt.afpgen.*68294*.xcel.afpds.UR*'
 
 
$ ur-support-query -stage prod -filterByDays 30 -getURItemInfoWithChildren 9163f64a-22a5-46be-b976-c28e17192ce3 > 68294.R.attributes
 
You should be able to figure it out from there.  I'm sorry if you  don't know linux; I don't have time to teach that.
 

# Main.py
from flask import Flask
from flask import escape
from flask import render_template
from werkzeug.datastructures import ImmutableMultiDict
import snowflake.connector
import tempfile

tempDir = tempfile.TemporaryDirectory();

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = tempfile.gettempdir()

def uploaderprocess(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <http://flask.pocoo.org/docs/1.0/api/#flask.Request>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>.
    """
    #request_json = request.get_json(silent=True)
    #request_args = request.args
    if request.method == 'POST':
        #print('********************************')
       	#print(request) 
        #print('================================')
       	#print(request.form) 
        #print('================================')
       	account = request.form['sf_account']
       	user = request.form['sf_user_id']
       	pwd = request.form['sf_user_pwd']
       	table = request.form['sf_table']
       	disp = request.form['sf_data_disposition']
        context = connectToSnowflake(account, user, pwd)
        #print(request.files.getlist('file-to-upload'))
        ii = request.files.getlist('file-to-upload')
        for i in ii:
            print(i.filename)
            i.save(tempfile.gettempdir() + '/' + i.filename)
            putToSnowflake(context, tempfile.gettempdir(),i.filename)
        copyIntoSnowflake(context,table,disp)
        removeFromSnowflake(context)
        print('********************************')
        tempDir.cleanup()
        return render_template('values.html',title='Uploader - Prototype',
             header='User Uploader Utility (U^3) - Prototype',
             a=account,u=user,n=len(ii),t=table)
             
    else:
        return 'NOT POST'

def copyIntoSnowflake(ctx, table, disp):
    sqlStmt = ''
    if disp == 'overwrite':
        sqlStmt = 'truncate table ' + table
        print(sqlStmt)
        ctx.cursor().execute(sqlStmt)
    sqlStmt = 'copy into ' + table + ' from @~/foobar'
    ctx.cursor().execute(sqlStmt)
 
def putToSnowflake(ctx, templocation, filename):
    sqlStmt = 'put file://' + templocation + '/' + filename + ' @~/foobar/' + filename + ' overwrite = true '
    #print(sqlStmt)
    ctx.cursor().execute(sqlStmt)

def removeFromSnowflake(ctx):
    sqlStmt = 'remove @~/foobar'
    print(sqlStmt)
    ctx.cursor().execute(sqlStmt)

def connectToSnowflake(account, user, password):
    ctx = snowflake.connector.connect(
        user=user,
        password=password,
        account=account
        )
    return ctx

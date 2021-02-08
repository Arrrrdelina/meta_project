from flask import Flask, render_template
from pony.orm import *
from main import Therapist


app = Flask(__name__)


@app.route('/')
def get_therapist():
    with db_session:
        therapists = Therapist.select().order_by(Therapist.id)
        return render_template('home.html', therapists=therapists)


if __name__ == "__main__":
    app.run(debug=True)

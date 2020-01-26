import tabula
import pandas

pdf_path = "Src/Polls/Focus/205_FOCUS_Hodnotenie parlamentnych volieb 2016 + Volebne preferencie_marec2016.pdf"

# readinf the PDF file that contain Table Data
df = tabula.read_pdf(pdf_path, pages=1)

print(df)

#tabula.read_pdf("offense.pdf", output_format="json")


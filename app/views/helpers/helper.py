import base64

def get_base64_image(image_path):
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()
    
def get_gif_image(image_path):
    image = f"data:image/gif;base64,{get_base64_image(image_path)}"
    return image
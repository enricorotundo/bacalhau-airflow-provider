use image::{open, GrayImage, Luma, Pixel};
use imageproc::definitions::Clamp;
use imageproc::gradients::sobel_gradient_map;
use imageproc::map::map_colors;
use imageproc::seam_carving::*;
use std::path::Path;

fn main() {
    // let input_path = "stable-diffusion/outputs/image0.png";
    // let input_path = "inputs/image0.png";
    let input_path = "inputs/outputs/image0.png";
    // let input_path = "image0.png";
    let output_dir = "outputs/";

    let input_path = Path::new(&input_path);
    let output_dir = Path::new(&output_dir);

    // Load image and convert to grayscale
    let input_image = open(input_path)
        .expect(&format!("Could not load image at {:?}", input_path))
        .to_rgb8();

    // Save original image in output directory
    let original_path = output_dir.join("original.png");
    input_image.save(&original_path).unwrap();

    // We will reduce the image width by this amount, removing one seam at a time.
    let seams_to_remove: u32 = input_image.width() / 6;

    let mut shrunk = input_image.clone();
    let mut seams = Vec::new();

    // Record each removed seam so that we can draw them on the original image later.
    for i in 0..seams_to_remove {
        if i % 100 == 0 {
            println!("Removing seam {}", i);
        }        
        let vertical_seam = find_vertical_seam(&shrunk);
        shrunk = remove_vertical_seam(&mut shrunk, &vertical_seam);
        seams.push(vertical_seam);
    }

    // Draw the seams on the original image.
    let gray_image = map_colors(&input_image, |p| p.to_luma());
    let annotated = draw_vertical_seams(&gray_image, &seams);
    let annotated_path = output_dir.join("annotated.png");
    annotated.save(&annotated_path).unwrap();

    // Draw the seams on the gradient magnitude image.
    let gradients = sobel_gradient_map(&input_image, |p| {
        let mean = (p[0] + p[1] + p[2]) / 3;
        Luma([mean as u32])
    });
    let clamped_gradients: GrayImage = map_colors(&gradients, |p| Luma([Clamp::clamp(p[0])]));
    let annotated_gradients = draw_vertical_seams(&clamped_gradients, &seams);
    let gradients_path = output_dir.join("gradients.png");
    clamped_gradients.save(&gradients_path).unwrap();
    let annotated_gradients_path = output_dir.join("annotated_gradients.png");
    annotated_gradients.save(&annotated_gradients_path).unwrap();

    // Save the shrunk image.
    let shrunk_path = output_dir.join("shrunk.png");
    shrunk.save(&shrunk_path).unwrap();
}